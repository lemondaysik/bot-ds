import os
import logging
import discord
from discord.ext import commands

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

ALLOWED_ROLE_ID = 1562452703274082424

def has_allowed_role(member: discord.Member) -> bool:
    return any(role.id == ALLOWED_ROLE_ID for role in member.roles)

@bot.event
async def on_ready():
    log.info(f"Бот {bot.user} готов")

@bot.command(name="send")
async def send_cmd(ctx: commands.Context, channel_id: int, *, text: str):
    if ctx.guild is None:
        await ctx.reply("Команда доступна только на сервере.")
        return

    if not has_allowed_role(ctx.author):
        await ctx.reply("У вас нет нужной роли для использования этой команды.")
        return

    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except discord.NotFound:
            await ctx.reply("Канал с таким ID не найден.")
            return
        except discord.Forbidden:
            await ctx.reply("У бота нет доступа к этому каналу.")
            return

    if not isinstance(channel, discord.TextChannel):
        await ctx.reply("Указанный ID не является текстовым каналом.")
        return

    bot_perms = channel.permissions_for(ctx.guild.me)
    if not bot_perms.send_messages:
        await ctx.reply(f"У бота нет прав писать в {channel.mention}.")
        return

    try:
        await channel.send(text)
        await ctx.reply(f"Сообщение отправлено в {channel.mention}")
        log.info(f"{ctx.author} отправил сообщение в #{channel.name}")
    except discord.Forbidden:
        await ctx.reply(f"У бота нет прав писать в {channel.mention}.")
    except Exception as e:
        log.exception("Ошибка отправки")
        await ctx.reply(f"Ошибка: {e}")

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("Переменная окружения DISCORD_TOKEN не задана!")

bot.run(TOKEN)