import os
import asyncio
import re
import logging
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional
import aiosqlite
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
ALLOWED_ROLE_IDS = {int(x) for x in os.getenv("ALLOWED_ROLE_IDS", "").split(",") if x.strip()}
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0")) or None
DB_PATH = os.getenv("DB_PATH", "bot.db")

MAX_MESSAGE_LEN = 2000
DEFAULT_MUTE_MINUTES = 10

WARN_LIMIT_FOR_MUTE = 3     
WARN_MUTE_DURATION = "2h"   

log = logging.getLogger("bot")
log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

console = logging.StreamHandler()
console.setFormatter(fmt)
log.addHandler(console)

fh = RotatingFileHandler("bot.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(fmt)
log.addHandler(fh)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    status=discord.Status.dnd,
    help_command=commands.DefaultHelpCommand(no_category="Команды"),
)


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript("""
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                author_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                guild_id INTEGER,
                content TEXT NOT NULL,
                message_id INTEGER,
                sent_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_author ON audit(author_id);
            CREATE INDEX IF NOT EXISTS idx_audit_channel ON audit(channel_id);

            CREATE TABLE IF NOT EXISTS scheduled (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                author_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                send_at TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sched_time ON scheduled(send_at, sent);

            CREATE TABLE IF NOT EXISTS bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                moderator_id INTEGER NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_bans_user ON bans(guild_id, user_id, active);

            CREATE TABLE IF NOT EXISTS mutes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                moderator_id INTEGER NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_mutes_user ON mutes(guild_id, user_id, active);

            -- НОВАЯ ТАБЛИЦА ДЛЯ ВАРНОВ
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                moderator_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_warnings_user ON warnings(guild_id, user_id, active);
        """)
        await self._conn.commit()
        log.info("БД готова: %s", self.path)

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def audit_add(self, author_id, channel_id, guild_id, content, message_id):
        await self._conn.execute(
            "INSERT INTO audit(author_id, channel_id, guild_id, content, message_id, sent_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (author_id, channel_id, guild_id, content, message_id,
             datetime.now(timezone.utc).isoformat()),
        )
        await self._conn.commit()

    async def audit_stats(self, guild_id: int):
        async with self._conn.execute(
            "SELECT COUNT(*) as total FROM audit WHERE guild_id = ?", (guild_id,)
        ) as cur:
            total = (await cur.fetchone())["total"]

        async with self._conn.execute(
            "SELECT channel_id, COUNT(*) as c FROM audit WHERE guild_id = ? "
            "GROUP BY channel_id ORDER BY c DESC LIMIT 5", (guild_id,)
        ) as cur:
            top_channels = await cur.fetchall()

        async with self._conn.execute(
            "SELECT author_id, COUNT(*) as c FROM audit WHERE guild_id = ? "
            "GROUP BY author_id ORDER BY c DESC LIMIT 5", (guild_id,)
        ) as cur:
            top_authors = await cur.fetchall()

        return total, top_channels, top_authors

    async def audit_history(self, guild_id: int, author_id: int, limit: int = 10):
        async with self._conn.execute(
            "SELECT * FROM audit WHERE guild_id = ? AND author_id = ? "
            "ORDER BY id DESC LIMIT ?", (guild_id, author_id, limit)
        ) as cur:
            return await cur.fetchall()

    async def schedule_add(self, author_id, channel_id, content, send_at: datetime):
        await self._conn.execute(
            "INSERT INTO scheduled(author_id, channel_id, content, send_at, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (author_id, channel_id, content, send_at.isoformat(),
             datetime.now(timezone.utc).isoformat()),
        )
        await self._conn.commit()

    async def schedule_due(self):
        now = datetime.now(timezone.utc).isoformat()
        async with self._conn.execute(
            "SELECT * FROM scheduled WHERE sent = 0 AND send_at <= ? ORDER BY send_at",
            (now,),
        ) as cur:
            return await cur.fetchall()

    async def schedule_mark_sent(self, row_id: int):
        await self._conn.execute("UPDATE scheduled SET sent = 1 WHERE id = ?", (row_id,))
        await self._conn.commit()

    async def schedule_list(self, guild_id: int, channel_ids: list[int]):
        if not channel_ids:
            return []
        placeholders = ",".join("?" * len(channel_ids))
        async with self._conn.execute(
            f"SELECT * FROM scheduled WHERE sent = 0 AND channel_id IN ({placeholders}) "
            "ORDER BY send_at", channel_ids
        ) as cur:
            return await cur.fetchall()

    async def schedule_cancel(self, row_id: int):
        await self._conn.execute("DELETE FROM scheduled WHERE id = ? AND sent = 0", (row_id,))
        await self._conn.commit()

    async def ban_add(self, guild_id, user_id, mod_id, reason, expires_at: Optional[datetime]):
        await self._conn.execute(
            "INSERT INTO bans(guild_id, user_id, moderator_id, reason, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (guild_id, user_id, mod_id, reason,
             datetime.now(timezone.utc).isoformat(),
             expires_at.isoformat() if expires_at else None),
        )
        await self._conn.commit()

    async def ban_active(self, guild_id, user_id) -> Optional[aiosqlite.Row]:
        async with self._conn.execute(
            "SELECT * FROM bans WHERE guild_id = ? AND user_id = ? AND active = 1 "
            "ORDER BY id DESC LIMIT 1", (guild_id, user_id)
        ) as cur:
            return await cur.fetchone()

    async def ban_deactivate(self, guild_id, user_id):
        await self._conn.execute(
            "UPDATE bans SET active = 0 WHERE guild_id = ? AND user_id = ? AND active = 1",
            (guild_id, user_id),
        )
        await self._conn.commit()

    async def bans_expired(self):
        now = datetime.now(timezone.utc).isoformat()
        async with self._conn.execute(
            "SELECT * FROM bans WHERE active = 1 AND expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        ) as cur:
            return await cur.fetchall()

    async def ban_history(self, guild_id, user_id, limit=10):
        async with self._conn.execute(
            "SELECT * FROM bans WHERE guild_id = ? AND user_id = ? ORDER BY id DESC LIMIT ?",
            (guild_id, user_id, limit),
        ) as cur:
            return await cur.fetchall()


    async def mute_add(self, guild_id, user_id, mod_id, reason, expires_at: Optional[datetime]):
        await self._conn.execute(
            "INSERT INTO mutes(guild_id, user_id, moderator_id, reason, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (guild_id, user_id, mod_id, reason,
             datetime.now(timezone.utc).isoformat(),
             expires_at.isoformat() if expires_at else None),
        )
        await self._conn.commit()

    async def mute_active(self, guild_id, user_id) -> Optional[aiosqlite.Row]:
        async with self._conn.execute(
            "SELECT * FROM mutes WHERE guild_id = ? AND user_id = ? AND active = 1 "
            "ORDER BY id DESC LIMIT 1", (guild_id, user_id)
        ) as cur:
            return await cur.fetchone()

    async def mute_deactivate(self, guild_id, user_id):
        await self._conn.execute(
            "UPDATE mutes SET active = 0 WHERE guild_id = ? AND user_id = ? AND active = 1",
            (guild_id, user_id),
        )
        await self._conn.commit()

    async def mutes_expired(self):
        now = datetime.now(timezone.utc).isoformat()
        async with self._conn.execute(
            "SELECT * FROM mutes WHERE active = 1 AND expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        ) as cur:
            return await cur.fetchall()

    async def mute_history(self, guild_id, user_id, limit=10):
        async with self._conn.execute(
            "SELECT * FROM mutes WHERE guild_id = ? AND user_id = ? ORDER BY id DESC LIMIT ?",
            (guild_id, user_id, limit),
        ) as cur:
            return await cur.fetchall()

    # ----- ВАРНЫ -----
    async def warn_add(self, guild_id, user_id, mod_id, reason):
        await self._conn.execute(
            "INSERT INTO warnings(guild_id, user_id, moderator_id, reason, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (guild_id, user_id, mod_id, reason, datetime.now(timezone.utc).isoformat()),
        )
        await self._conn.commit()

    async def warns_active(self, guild_id, user_id):
        async with self._conn.execute(
            "SELECT * FROM warnings WHERE guild_id = ? AND user_id = ? AND active = 1 "
            "ORDER BY id DESC", (guild_id, user_id)
        ) as cur:
            return await cur.fetchall()

    async def warns_count(self, guild_id, user_id) -> int:
        async with self._conn.execute(
            "SELECT COUNT(*) as c FROM warnings WHERE guild_id = ? AND user_id = ? AND active = 1",
            (guild_id, user_id)
        ) as cur:
            return (await cur.fetchone())["c"]

    async def warn_remove(self, guild_id, user_id, warn_id):
        await self._conn.execute(
            "UPDATE warnings SET active = 0 WHERE id = ? AND guild_id = ? AND user_id = ?",
            (warn_id, guild_id, user_id),
        )
        await self._conn.commit()

    async def warns_clear(self, guild_id, user_id):
        await self._conn.execute(
            "UPDATE warnings SET active = 0 WHERE guild_id = ? AND user_id = ? AND active = 1",
            (guild_id, user_id),
        )
        await self._conn.commit()


db = Database(DB_PATH)


def has_allowed_role(member: discord.Member) -> bool:
    return any(role.id in ALLOWED_ROLE_IDS for role in member.roles)


def is_allowed():
    async def predicate(ctx: commands.Context) -> bool:
        if ctx.guild is None:
            raise commands.CheckFailure("Команда доступна только на сервере.")
        if not has_allowed_role(ctx.author):
            raise commands.CheckFailure("Нет нужной роли.")
        return True
    return commands.check(predicate)


def split_message(text: str, limit: int = MAX_MESSAGE_LEN) -> list[str]:
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = text.rfind(" ", 0, limit)
        if cut == -1:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    parts.append(text)
    return parts


def parse_duration(s: str) -> Optional[timedelta]:
    if not s:
        return None
    s = s.strip().lower()
    if len(s) < 2:
        return None
    num, unit = s[:-1], s[-1]
    if not num.isdigit():
        return None
    n = int(num)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    if unit not in multipliers:
        return None
    return timedelta(seconds=n * multipliers[unit])


def fmt_dt(iso: Optional[str]) -> str:
    if not iso:
        return "навсегда"
    dt = datetime.fromisoformat(iso)
    return discord.utils.format_dt(dt, style="R")


async def log_action(ctx: commands.Context, action: str, extra: str = ""):
    if LOG_CHANNEL_ID:
        ch = bot.get_channel(LOG_CHANNEL_ID)
        if ch:
            try:
                await ch.send(f"`{ctx.author}` ({ctx.author.id}) — {action}. {extra}")
            except discord.Forbidden:
                log.warning("Нет прав писать в лог-канал")


@bot.event
async def on_ready():
    await db.connect()
    check_expirations.start()
    check_scheduled.start()
    log.info(f"Бот {bot.user} (id={bot.user.id}) готов. Гильдий: {len(bot.guilds)}")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(
            f"Не хватает аргумента `{error.param.name}`.\n"
            f"Использование: `{ctx.prefix}{ctx.command.name} {ctx.command.signature}`"
        )
    elif isinstance(error, commands.BadArgument):
        await ctx.reply(f"Неверный аргумент: {error}")
    elif isinstance(error, commands.MissingPermissions):
        await ctx.reply("У вас недостаточно прав для этой команды.")
    elif isinstance(error, commands.CheckFailure):
        await ctx.reply(f"Проверка не пройдена: {error}")
    elif isinstance(error, commands.CommandNotFound):
        return
    else:
        log.exception("Необработанная ошибка команды", exc_info=error)
        await ctx.reply(f"Ошибка: `{type(error).__name__}`.")


RR_REPLY_ROLE_ID = 1572630321680224347

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if re.search(r'\bRR\b', message.content, re.IGNORECASE):
        has_role = any(role.id == RR_REPLY_ROLE_ID for role in message.author.roles)
        if not has_role:
            await message.reply("Пошел нахуй если не шаришь")

    await bot.process_commands(message)


@tasks.loop(seconds=30)
async def check_scheduled():
    try:
        rows = await db.schedule_due()
        for row in rows:
            ch = bot.get_channel(row["channel_id"])
            if not ch:
                try:
                    ch = await bot.fetch_channel(row["channel_id"])
                except Exception:
                    log.warning("Канал %s для отложки не найден", row["channel_id"])
                    await db.schedule_mark_sent(row["id"])
                    continue
            try:
                for part in split_message(row["content"]):
                    await ch.send(
                        part,
                        allowed_mentions=discord.AllowedMentions(
                            everyone=False, roles=False, users=True
                        ),
                    )
                await db.schedule_mark_sent(row["id"])
                await db.audit_add(row["author_id"], row["channel_id"], ch.guild.id if ch.guild else None,
                                   row["content"], None)
                log.info("Отложка #%s отправлена в %s", row["id"], row["channel_id"])
            except Exception as e:
                log.exception("Ошибка отправки отложки #%s: %s", row["id"], e)
    except Exception:
        log.exception("Ошибка в check_scheduled")


@tasks.loop(seconds=60)
async def check_expirations():
    try:
        for row in await db.bans_expired():
            guild = bot.get_guild(row["guild_id"])
            if guild:
                try:
                    await guild.unban(discord.Object(id=row["user_id"]),
                                      reason="Истёк срок бана")
                except discord.NotFound:
                    pass
                except Exception:
                    log.exception("Не удалось разбанить %s", row["user_id"])
            await db.ban_deactivate(row["guild_id"], row["user_id"])
            log.info("Бан снят по истечении: %s/%s", row["guild_id"], row["user_id"])

        for row in await db.mutes_expired():
            guild = bot.get_guild(row["guild_id"])
            if guild:
                member = guild.get_member(row["user_id"])
                if member:
                    try:
                        await member.timeout(None, reason="Истёк срок мута")
                    except Exception:
                        log.exception("Не удалось снять мут %s", row["user_id"])
            await db.mute_deactivate(row["guild_id"], row["user_id"])
            log.info("Мут снят по истечении: %s/%s", row["guild_id"], row["user_id"])
    except Exception:
        log.exception("Ошибка в check_expirations")


@bot.hybrid_command(name="send", description="Отправить сообщение в указанный канал")
@commands.guild_only()
@is_allowed()
async def send_cmd(ctx: commands.Context, channel: discord.TextChannel, *, text: str):
    async with ctx.typing():
        perms = channel.permissions_for(ctx.guild.me)
        if not perms.send_messages:
            await ctx.reply(f"У бота нет прав писать в {channel.mention}.")
            return

        parts = split_message(text)
        sent_ids = []
        try:
            for part in parts:
                msg = await channel.send(
                    part,
                    allowed_mentions=discord.AllowedMentions(
                        everyone=False, roles=False, users=True
                    ),
                )
                sent_ids.append(msg.id)
        except discord.Forbidden:
            await ctx.reply(f"У бота нет прав писать в {channel.mention}.")
            return
        except Exception as e:
            log.exception("Ошибка отправки")
            await ctx.reply(f"Ошибка: `{e}`")
            return

        await db.audit_add(ctx.author.id, channel.id, ctx.guild.id, text, sent_ids[0] if sent_ids else None)
        await ctx.reply(f"✅ Отправлено в {channel.mention}")
        await log_action(ctx, f"send → {channel.mention}", f"частей: {len(parts)}")


@bot.hybrid_command(name="sendembed", description="Отправить embed в канал")
@commands.guild_only()
@is_allowed()
async def send_embed(
    ctx: commands.Context,
    channel: discord.TextChannel,
    title: str,
    *,
    description: str = "",
):
    embed = discord.Embed(
        title=title,
        description=description or None,
        color=discord.Color.blurple(),
        timestamp=discord.utils.utcnow(),
    )
    embed.set_footer(text=f"Отправлено: {ctx.author.display_name}")
    try:
        msg = await channel.send(embed=embed)
        await db.audit_add(ctx.author.id, channel.id, ctx.guild.id, f"[embed] {title}", msg.id)
        await ctx.reply(f"✅ Embed отправлен в {channel.mention}")
        await log_action(ctx, f"sendembed → {channel.mention}")
    except discord.Forbidden:
        await ctx.reply(f"У бота нет прав писать в {channel.mention}.")


@bot.hybrid_command(name="say", description="Повторить текст в текущем канале")
@commands.guild_only()
@is_allowed()
async def say_cmd(ctx: commands.Context, *, text: str):
    try:
        await ctx.message.delete()
    except Exception:
        pass
    sent_ids = []
    for part in split_message(text):
        msg = await ctx.send(part, allowed_mentions=discord.AllowedMentions.none())
        sent_ids.append(msg.id)
    await db.audit_add(ctx.author.id, ctx.channel.id, ctx.guild.id, text,
                       sent_ids[0] if sent_ids else None)


@bot.hybrid_command(name="ping", description="Проверить задержку бота")
async def ping_cmd(ctx: commands.Context):
    await ctx.reply(f"🏓 Pong! `{round(bot.latency * 1000)}ms`")


@bot.hybrid_command(name="uptime", description="Сколько бот уже работает")
async def uptime_cmd(ctx: commands.Context):
    delta = datetime.now(timezone.utc) - bot.launch_time
    hours, rem = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(rem, 60)
    await ctx.reply(f"⏱ Uptime: **{hours}ч {minutes}м {seconds}с**")


@bot.hybrid_command(name="schedule", description="Отложенная отправка: !schedule <канал> <10m|2h|1d> <текст>")
@commands.guild_only()
@is_allowed()
async def schedule_cmd(ctx: commands.Context, channel: discord.TextChannel, when: str, *, text: str):
    delta = parse_duration(when)
    if not delta:
        await ctx.reply("Неверный формат времени. Примеры: `30s`, `10m`, `2h`, `1d`, `1w`.")
        return
    if len(text) > MAX_MESSAGE_LEN:
        await ctx.reply("Текст длиннее 2000 символов — сократите.")
        return

    perms = channel.permissions_for(ctx.guild.me)
    if not perms.send_messages:
        await ctx.reply(f"У бота нет прав писать в {channel.mention}.")
        return

    send_at = datetime.now(timezone.utc) + delta
    await db.schedule_add(ctx.author.id, channel.id, text, send_at)
    await ctx.reply(
        f"📅 Запланировано в {channel.mention} на {discord.utils.format_dt(send_at, 'F')} "
        f"({discord.utils.format_dt(send_at, 'R')})"
    )
    await log_action(ctx, f"schedule → {channel.mention}", f"через {when}")


@bot.hybrid_command(name="scheduled", description="Показать отложенные сообщения на этом сервере")
@commands.guild_only()
@is_allowed()
async def scheduled_list(ctx: commands.Context):
    channel_ids = [ch.id for ch in ctx.guild.channels if isinstance(ch, discord.TextChannel)]
    rows = await db.schedule_list(ctx.guild.id, channel_ids)
    if not rows:
        await ctx.reply("Отложенных сообщений нет.")
        return

    lines = ["**Отложенные сообщения:**"]
    for row in rows[:20]:
        ch = bot.get_channel(row["channel_id"])
        ch_mention = ch.mention if ch else f"`{row['channel_id']}`"
        preview = row["content"][:40] + ("…" if len(row["content"]) > 40 else "")
        lines.append(
            f"`#{row['id']}` → {ch_mention} — {fmt_dt(row['send_at'])} — «{preview}»"
        )
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="unschedule", description="Отменить отложенное сообщение по ID")
@commands.guild_only()
@is_allowed()
async def unschedule_cmd(ctx: commands.Context, schedule_id: int):
    channel_ids = [ch.id for ch in ctx.guild.channels if isinstance(ch, discord.TextChannel)]
    rows = await db.schedule_list(ctx.guild.id, channel_ids)
    if not any(r["id"] == schedule_id for r in rows):
        await ctx.reply("Отложка с таким ID не найдена на этом сервере.")
        return
    await db.schedule_cancel(schedule_id)
    await ctx.reply(f"🗑 Отложка `#{schedule_id}` отменена.")
    await log_action(ctx, f"unschedule #{schedule_id}")


@bot.hybrid_command(name="stats", description="Статистика отправленных сообщений на сервере")
@commands.guild_only()
@is_allowed()
async def stats_cmd(ctx: commands.Context):
    total, top_channels, top_authors = await db.audit_stats(ctx.guild.id)

    lines = [f"**Всего отправлено сообщений через бота:** {total}"]

    if top_channels:
        lines.append("\n**Топ каналов:**")
        for row in top_channels:
            ch = bot.get_channel(row["channel_id"])
            mention = ch.mention if ch else f"`{row['channel_id']}`"
            lines.append(f"• {mention} — {row['c']}")

    if top_authors:
        lines.append("\n**Топ авторов:**")
        for row in top_authors:
            user = bot.get_user(row["author_id"])
            name = user.mention if user else f"`{row['author_id']}`"
            lines.append(f"• {name} — {row['c']}")

    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="history", description="Последние сообщения пользователя через бота")
@commands.guild_only()
@is_allowed()
async def history_cmd(ctx: commands.Context, member: discord.Member, limit: int = 10):
    limit = max(1, min(limit, 50))
    rows = await db.audit_history(ctx.guild.id, member.id, limit)
    if not rows:
        await ctx.reply(f"У {member.mention} нет записей.")
        return

    lines = [f"**История {member.mention}** (последние {len(rows)}):"]
    for row in rows:
        ch = bot.get_channel(row["channel_id"])
        mention = ch.mention if ch else f"`{row['channel_id']}`"
        preview = row["content"][:60] + ("…" if len(row["content"]) > 60 else "")
        lines.append(f"• {fmt_dt(row['sent_at'])} → {mention} — «{preview}»")
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="ban", description="Забанить: !ban <юзер> [10m|2h|1d|навсегда] [причина]")
@commands.guild_only()
@is_allowed()
@commands.bot_has_permissions(ban_members=True)
async def ban_cmd(ctx: commands.Context, member: discord.Member, duration: Optional[str] = None, *, reason: str = "не указана"):
    if member == ctx.author:
        await ctx.reply("Себя банить не надо 🙂")
        return
    if member.top_role >= ctx.guild.me.top_role:
        await ctx.reply("Не могу забанить — роль пользователя выше или равна моей.")
        return
    if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
        await ctx.reply("Нельзя банить пользователя с ролью выше или равной вашей.")
        return

    expires_at = None
    if duration and duration.lower() not in ("навсегда", "forever", "perm", "permanent"):
        delta = parse_duration(duration)
        if not delta:
            await ctx.reply("Неверный формат срока. Примеры: `10m`, `2h`, `1d`, `навсегда`.")
            return
        expires_at = datetime.now(timezone.utc) + delta

    try:
        await member.ban(reason=f"{ctx.author}: {reason}", delete_message_days=0)
    except discord.Forbidden:
        await ctx.reply("Не хватает прав забанить.")
        return
    except Exception as e:
        await ctx.reply(f"Ошибка: `{e}`")
        return

    await db.ban_add(ctx.guild.id, member.id, ctx.author.id, reason, expires_at)

    suffix = f" до {discord.utils.format_dt(expires_at, 'F')}" if expires_at else " навсегда"
    await ctx.reply(f"🔨 {member.mention} забанен{suffix}. Причина: {reason}")
    await log_action(ctx, f"ban → {member} ({member.id})", f"срок: {duration or 'навсегда'}, причина: {reason}")


@bot.hybrid_command(name="unban", description="Разбанить по ID: !unban <user_id> [причина]")
@commands.guild_only()
@is_allowed()
@commands.bot_has_permissions(ban_members=True)
async def unban_cmd(ctx: commands.Context, user_id: int, *, reason: str = "не указана"):
    try:
        await ctx.guild.unban(discord.Object(id=user_id), reason=f"{ctx.author}: {reason}")
    except discord.NotFound:
        await ctx.reply("Этот пользователь не забанен.")
        return
    except discord.Forbidden:
        await ctx.reply("Не хватает прав разбанить.")
        return

    await db.ban_deactivate(ctx.guild.id, user_id)
    await ctx.reply(f"✅ Пользователь `{user_id}` разбанен.")
    await log_action(ctx, f"unban → {user_id}", f"причина: {reason}")


@bot.hybrid_command(name="baninfo", description="Информация о бане: !baninfo <user_id>")
@commands.guild_only()
@is_allowed()
async def baninfo_cmd(ctx: commands.Context, user_id: int):
    row = await db.ban_active(ctx.guild.id, user_id)
    if not row:
        await ctx.reply("Активного бана нет.")
        return
    user = bot.get_user(user_id)
    name = user.mention if user else f"`{user_id}`"
    lines = [
        f"**Бан {name}**",
        f"Модератор: <@{row['moderator_id']}>",
        f"Причина: {row['reason'] or '—'}",
        f"Выдан: {fmt_dt(row['created_at'])}",
        f"Истекает: {fmt_dt(row['expires_at'])}",
    ]
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="banhistory", description="История банов: !banhistory <user_id>")
@commands.guild_only()
@is_allowed()
async def banhistory_cmd(ctx: commands.Context, user_id: int, limit: int = 10):
    limit = max(1, min(limit, 50))
    rows = await db.ban_history(ctx.guild.id, user_id, limit)
    if not rows:
        await ctx.reply("История пуста.")
        return
    lines = [f"**История банов `{user_id}`:**"]
    for row in rows:
        status = "🟢 активен" if row["active"] else "⚪ снят"
        lines.append(
            f"• `#{row['id']}` {status} — {row['reason'] or '—'} — "
            f"мод: <@{row['moderator_id']}> — до {fmt_dt(row['expires_at'])}"
        )
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="mute", description="Замутить: !mute <юзер> <10m|2h|1d> [причина]")
@commands.guild_only()
@is_allowed()
@commands.bot_has_permissions(moderate_members=True)
async def mute_cmd(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = "не указана"):
    if member == ctx.author:
        await ctx.reply("Себя мутить не надо 🙂")
        return
    if member.top_role >= ctx.guild.me.top_role:
        await ctx.reply("Не могу замутить — роль пользователя выше или равна моей.")
        return
    if member.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
        await ctx.reply("Нельзя мутить пользователя с ролью выше или равной вашей.")
        return

    delta = parse_duration(duration)
    if not delta:
        await ctx.reply("Неверный срок. Примеры: `30s`, `10m`, `2h`, `1d`.")
        return
    if delta.total_seconds() > 28 * 86400:
        await ctx.reply("Discord разрешает таймаут максимум на 28 дней.")
        return

    expires_at = datetime.now(timezone.utc) + delta
    try:
        await member.timeout(delta, reason=f"{ctx.author}: {reason}")
    except discord.Forbidden:
        await ctx.reply("Не хватает прав замутить.")
        return
    except Exception as e:
        await ctx.reply(f"Ошибка: `{e}`")
        return

    await db.mute_add(ctx.guild.id, member.id, ctx.author.id, reason, expires_at)
    await ctx.reply(
        f"🔇 {member.mention} замучен до {discord.utils.format_dt(expires_at, 'F')} "
        f"({discord.utils.format_dt(expires_at, 'R')}). Причина: {reason}"
    )
    await log_action(ctx, f"mute → {member} ({member.id})", f"срок: {duration}, причина: {reason}")


@bot.hybrid_command(name="unmute", description="Снять мут: !unmute <юзер> [причина]")
@commands.guild_only()
@is_allowed()
@commands.bot_has_permissions(moderate_members=True)
async def unmute_cmd(ctx: commands.Context, member: discord.Member, *, reason: str = "не указана"):
    try:
        await member.timeout(None, reason=f"{ctx.author}: {reason}")
    except discord.Forbidden:
        await ctx.reply("Не хватает прав снять мут.")
        return
    except Exception as e:
        await ctx.reply(f"Ошибка: `{e}`")
        return

    await db.mute_deactivate(ctx.guild.id, member.id)
    await ctx.reply(f"🔊 {member.mention} размучен.")
    await log_action(ctx, f"unmute → {member} ({member.id})", f"причина: {reason}")


@bot.hybrid_command(name="muteinfo", description="Информация о муте: !muteinfo <юзер>")
@commands.guild_only()
@is_allowed()
async def muteinfo_cmd(ctx: commands.Context, member: discord.Member):
    row = await db.mute_active(ctx.guild.id, member.id)
    if not row:
        await ctx.reply("Активного мута нет.")
        return
    lines = [
        f"**Мут {member.mention}**",
        f"Модератор: <@{row['moderator_id']}>",
        f"Причина: {row['reason'] or '—'}",
        f"Выдан: {fmt_dt(row['created_at'])}",
        f"Истекает: {fmt_dt(row['expires_at'])}",
    ]
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="mutehistory", description="История мутов: !mutehistory <юзер>")
@commands.guild_only()
@is_allowed()
async def mutehistory_cmd(ctx: commands.Context, member: discord.Member, limit: int = 10):
    limit = max(1, min(limit, 50))
    rows = await db.mute_history(ctx.guild.id, member.id, limit)
    if not rows:
        await ctx.reply("История пуста.")
        return
    lines = [f"**История мутов {member.mention}:**"]
    for row in rows:
        status = "🟢 активен" if row["active"] else "⚪ снят"
        lines.append(
            f"• `#{row['id']}` {status} — {row['reason'] or '—'} — "
            f"мод: <@{row['moderator_id']}> — до {fmt_dt(row['expires_at'])}"
        )
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="warn", description="Выдать предупреждение: !warn <юзер> <причина>")
@commands.guild_only()
@is_allowed()
async def warn_cmd(ctx: commands.Context, member: discord.Member, *, reason: str = "не указана"):
    if member == ctx.author:
        await ctx.reply("Себя предупреждать не надо 🙂")
        return
    if member.bot:
        await ctx.reply("Ботам варны не выдают.")
        return

    await db.warn_add(ctx.guild.id, member.id, ctx.author.id, reason)
    count = await db.warns_count(ctx.guild.id, member.id)

    mute_msg = ""
    if count >= WARN_LIMIT_FOR_MUTE:
        delta = parse_duration(WARN_MUTE_DURATION)
        if delta:
            try:
                await member.timeout(delta, reason=f"Автомут за {count} предупреждений")
                expires_at = datetime.now(timezone.utc) + delta
                await db.mute_add(ctx.guild.id, member.id, bot.user.id, f"Автомут ({count} варнов)", expires_at)
                mute_msg = f"\n🤖 **Автомут:** пользователь замучен на {WARN_MUTE_DURATION} за {count} предупреждений."
            except Exception as e:
                log.exception("Ошибка автомута")
                mute_msg = f"\n⚠️ Не удалось выдать автомут: {e}"

    await ctx.reply(
        f"⚠️ {member.mention} получил предупреждение. Причина: {reason}\n"
        f"Всего предупреждений: **{count}/{WARN_LIMIT_FOR_MUTE}**{mute_msg}"
    )
    await log_action(ctx, f"warn → {member} ({member.id})", f"причина: {reason} (всего: {count})")


@bot.hybrid_command(name="warns", description="Показать активные варны юзера: !warns <юзер>")
@commands.guild_only()
@is_allowed()
async def warns_cmd(ctx: commands.Context, member: discord.Member):
    rows = await db.warns_active(ctx.guild.id, member.id)
    if not rows:
        await ctx.reply(f"У {member.mention} нет активных предупреждений.")
        return

    lines = [f"**Предупреждения {member.mention}** (всего: {len(rows)}):"]
    for row in rows:
        lines.append(
            f"• `#{row['id']}` — {row['reason']} — "
            f"мод: <@{row['moderator_id']}> — {fmt_dt(row['created_at'])}"
        )
    await ctx.reply("\n".join(lines))


@bot.hybrid_command(name="unwarn", description="Снять варн по ID: !unwarn <юзер> <id_варна>")
@commands.guild_only()
@is_allowed()
async def unwarn_cmd(ctx: commands.Context, member: discord.Member, warn_id: int):
    rows = await db.warns_active(ctx.guild.id, member.id)
    if not any(r["id"] == warn_id for r in rows):
        await ctx.reply("Активный варн с таким ID не найден у этого пользователя.")
        return
    
    await db.warn_remove(ctx.guild.id, member.id, warn_id)
    await ctx.reply(f"✅ Варн `#{warn_id}` снят с {member.mention}.")
    await log_action(ctx, f"unwarn → {member} ({member.id})", f"снят варн #{warn_id}")


@bot.hybrid_command(name="clearwarns", description="Снять ВСЕ варны с пользователя: !clearwarns <юзер>")
@commands.guild_only()
@is_allowed()
async def clearwarns_cmd(ctx: commands.Context, member: discord.Member):
    rows = await db.warns_active(ctx.guild.id, member.id)
    if not rows:
        await ctx.reply(f"У {member.mention} и так нет активных варнов.")
        return

    await db.warns_clear(ctx.guild.id, member.id)
    await ctx.reply(f"🗑 Все варны ({len(rows)} шт.) сняты с {member.mention}.")
    await log_action(ctx, f"clearwarns → {member} ({member.id})", f"снято {len(rows)} шт.")


bot.launch_time = datetime.now(timezone.utc)

if not TOKEN:
    raise RuntimeError("Переменная окружения DISCORD_TOKEN не задана!")

try:
    bot.run(TOKEN)
finally:
    pass
