from calendar import c
from aiohttp import ClientSession
from apscheduler.triggers.interval import IntervalTrigger
from asyncio import Lock, sleep
from datetime import datetime, timedelta
from feedparser import parse as feedparse
from functools import partial
from io import BytesIO
from pyrogram.filters import command, regex, create
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from re import split as re_split, sub as re_sub
from time import time

from bot import scheduler, rss_dict, LOGGER, DATABASE_URL, config_dict, bot
from bot.helper.ext_utils.bot_utils import new_thread
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.ext_utils.exceptions import RssShutdownException
from bot.helper.ext_utils.help_messages import RSS_HELP_MESSAGE
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.message_utils import (
    sendMessage,
    editMessage,
    sendRss,
    sendFile,
    deleteMessage,
)

rss_dict_lock = Lock()
handler_dict = {}


async def rssMenu(event):
    user_id = event.from_user.id
    buttons = ButtonMaker()
    buttons.ibutton("Subscribe", f"rss sub {user_id}")
    buttons.ibutton("Subscriptions", f"rss list {user_id} 0")
    buttons.ibutton("Get Items", f"rss get {user_id}")
    buttons.ibutton("Edit", f"rss edit {user_id}")
    buttons.ibutton("Pause", f"rss pause {user_id}")
    buttons.ibutton("Resume", f"rss resume {user_id}")
    buttons.ibutton("Unsubscribe", f"rss unsubscribe {user_id}")
    if await CustomFilters.sudo("", event):
        buttons.ibutton("All Subscriptions", f"rss listall {user_id} 0")
        buttons.ibutton("Pause All", f"rss allpause {user_id}")
        buttons.ibutton("Resume All", f"rss allresume {user_id}")
        buttons.ibutton("Unsubscribe All", f"rss allunsub {user_id}")
        buttons.ibutton("Delete User", f"rss deluser {user_id}")
        if scheduler.running:
            buttons.ibutton("Shutdown Rss", f"rss shutdown {user_id}")
        else:
            buttons.ibutton("Start Rss", f"rss start {user_id}")
    buttons.ibutton("Close", f"rss close {user_id}")
    button = buttons.build_menu(2)
    msg = f"Rss Menu | Users: {len(rss_dict)} | Running: {scheduler.running}"
    return msg, button


async def rssGet(_, message, pre_event):
    user_id = message.from_user.id
    handler_dict[user_id] = False
    args = message.text.split()
    if len(args) < 2:
        await sendMessage(
            message,
            f"{args}. Wrong Input format. You should add number of the items you want to get. Read help message before adding new subcription!",
        )
        await updateRssMenu(pre_event)
        return
    try:
        title = args[0]
        count = int(args[1])
        data = rss_dict[user_id].get(title, False)
        if data and count > 0:
            try:
                msg = await sendMessage(
                    message, f"Getting the last <b>{count}</b> item(s) from {title}"
                )
                async with ClientSession() as session:
                    async with session.get(data["link"], ssl=False) as res:
                        html = await res.text()
                rss_d = feedparse(html)
                item_info = ""
                for item_num in range(count):
                    try:
                        link = rss_d.entries[item_num]["links"][1]["href"]
                    except IndexError:
                        link = rss_d.entries[item_num]["link"]
                    item_info += f"<b>Name: </b><code>{rss_d.entries[item_num]['title'].replace('>', '').replace('<', '')}</code>\n"
                    item_info += f"<b>Link: </b><code>{link}</code>\n\n"
                item_info_ecd = item_info.encode()
                if len(item_info_ecd) > 4000:
                    with BytesIO(item_info_ecd) as out_file:
                        out_file.name = f"rssGet {title} items_no. {count}.txt"
                        await sendFile(message, out_file)
                    await deleteMessage(msg)
                else:
                    await editMessage(msg, item_info)
            except IndexError as e:
                LOGGER.error(str(e))
                await editMessage(
                    msg, "Parse depth exceeded. Try again with a lower value."
                )
            except Exception as e:
                LOGGER.error(str(e))
                await editMessage(msg, str(e))
        else:
            await sendMessage(message, "Enter a valid title. Title not found!")
    except Exception as e:
        LOGGER.error(str(e))
        await sendMessage(message, f"Enter a valid value!. {e}")
    await updateRssMenu(pre_event)


async def rssEdit(_, message, pre_event):
    user_id = message.from_user.id
    handler_dict[user_id] = False
    items = message.text.split("\n")
    updated = False
    for item in items:
        args = item.split()
        title = args[0].strip()
        if len(args) < 2:
            await sendMessage(
                message,
                f"{item}. Wrong Input format. Read help message before editing!",
            )
            continue
        elif not rss_dict[user_id].get(title, False):
            await sendMessage(message, "Enter a valid title. Title not found!")
            continue
        updated = True
        inf_lists = []
        exf_lists = []
        arg = item.split(" -c ", 1)
        cmd = re_split(" -inf | -exf ", arg[1])[0].strip() if len(arg) > 1 else None
        arg = item.split(" -inf ", 1)
        inf = re_split(" -c | -exf ", arg[1])[0].strip() if len(arg) > 1 else None
        arg = item.split(" -exf ", 1)
        exf = re_split(" -c | -inf ", arg[1])[0].strip() if len(arg) > 1 else None
        async with rss_dict_lock:
            if cmd is not None:
                if cmd.lower() == "none":
                    cmd = None
                rss_dict[user_id][title]["command"] = cmd
            if inf is not None:
                if inf.lower() != "none":
                    filters_list = inf.split("|")
                    for x in filters_list:
                        y = x.split(" or ")
                        inf_lists.append(y)
                rss_dict[user_id][title]["inf"] = inf_lists
            if exf is not None:
                if exf.lower() != "none":
                    filters_list = exf.split("|")
                    for x in filters_list:
                        y = x.split(" or ")
                        exf_lists.append(y)
                rss_dict[user_id][title]["exf"] = exf_lists
    if DATABASE_URL and updated:
        await DbManager().rss_update(user_id)
        await updateRssMenu(pre_event)


async def rssDelete(_, message, pre_event):
    handler_dict[message.from_user.id] = False
    users = message.text.split()
    for user in users:
        user = int(user)
        async with rss_dict_lock:
            del rss_dict[user]
        if DATABASE_URL:
            await DbManager().rss_delete(user)
    await updateRssMenu(pre_event)


async def event_handler(client, query, pfunc):
    user_id = query.from_user.id
    handler_dict[user_id] = True
    start_time = time()

    async def event_filter(_, __, event):
        user = event.from_user or event.sender_chat
        return bool(
            user.id == user_id and event.chat.id == query.message.chat.id and event.text
        )

    handler = client.add_handler(MessageHandler(pfunc, create(event_filter)), group=-1)
    while handler_dict[user_id]:
        await sleep(0.5)
        if time() - start_time > 60:
            handler_dict[user_id] = False
            await updateRssMenu(query)
    client.remove_handler(*handler)


@new_thread
async def rssListener(client, query):
    user_id = query.from_user.id
    message = query.message
    data = query.data.split()
    if int(data[2]) != user_id and not await CustomFilters.sudo("", query):
        await query.answer(
            text="You don't have permission to use these buttons!", show_alert=True
        )
    elif data[1] == "close":
        await query.answer()
        handler_dict[user_id] = False
        await deleteMessage(message.reply_to_message)
        await deleteMessage(message)
    elif data[1] == "back":
        await query.answer()
        handler_dict[user_id] = False
        await updateRssMenu(query)
    elif data[1] == "sub":
        await query.answer()
        handler_dict[user_id] = False
        buttons = ButtonMaker()
        buttons.ibutton("Back", f"rss back {user_id}")
        buttons.ibutton("Close", f"rss close {user_id}")
        button = buttons.build_menu(2)
        await editMessage(message, RSS_HELP_MESSAGE, button)
        pfunc = partial(rssSub, pre_event=query)
        await event_handler(client, query, pfunc)
    elif data[1] == "list":
        handler_dict[user_id] = False
        if len(rss_dict.get(int(data[2]), {})) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
        else:
            await query.answer()
            start = int(data[3])
            await rssList(query, start)
    elif data[1] == "get":
        handler_dict[user_id] = False
        if len(rss_dict.get(int(data[2]), {})) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
        else:
            await query.answer()
            buttons = ButtonMaker()
            buttons.ibutton("Back", f"rss back {user_id}")
            buttons.ibutton("Close", f"rss close {user_id}")
            button = buttons.build_menu(2)
            await editMessage(
                message,
                "Send one title with value separated by space get last X items.\nTitle Value\nTimeout: 60 sec.",
                button,
            )
            pfunc = partial(rssGet, pre_event=query)
            await event_handler(client, query, pfunc)
    elif data[1] in ["unsubscribe", "pause", "resume"]:
        handler_dict[user_id] = False
        if len(rss_dict.get(int(data[2]), {})) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
        else:
            await query.answer()
            buttons = ButtonMaker()
            buttons.ibutton("Back", f"rss back {user_id}")
            if data[1] == "pause":
                buttons.ibutton("Pause AllMyFeeds", f"rss uallpause {user_id}")
            elif data[1] == "resume":
                buttons.ibutton("Resume AllMyFeeds", f"rss uallresume {user_id}")
            elif data[1] == "unsubscribe":
                buttons.ibutton("Unsub AllMyFeeds", f"rss uallunsub {user_id}")
            buttons.ibutton("Close", f"rss close {user_id}")
            button = buttons.build_menu(2)
            await editMessage(
                message,
                f"Send one or more rss titles separated by space to {data[1]}.\nTimeout: 60 sec.",
                button,
            )
            pfunc = partial(rssUpdate, pre_event=query, state=data[1])
            await event_handler(client, query, pfunc)
    elif data[1] == "edit":
        handler_dict[user_id] = False
        if len(rss_dict.get(int(data[2]), {})) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
        else:
            await query.answer()
            buttons = ButtonMaker()
            buttons.ibutton("Back", f"rss back {user_id}")
            buttons.ibutton("Close", f"rss close {user_id}")
            button = buttons.build_menu(2)
            msg = """Send one or more rss titles with new filters or command separated by new line.
Examples:
Title1 -c mirror -up remote:path/subdir -exf none -inf 1080 or 720
Title2 -c none -inf none
Title3 -c mirror -rcf xyz -up xyz -z pswd
Note: Only what you provide will be edited, the rest will be the same like example 2: exf will stay same as it is.
Timeout: 60 sec. Argument -c for command and arguments
            """
            await editMessage(message, msg, button)
            pfunc = partial(rssEdit, pre_event=query)
            await event_handler(client, query, pfunc)
    elif data[1].startswith("uall"):
        handler_dict[user_id] = False
        if len(rss_dict.get(int(data[2]), {})) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
            return
        await query.answer()
        if data[1].endswith("unsub"):
            async with rss_dict_lock:
                del rss_dict[int(data[2])]
            if DATABASE_URL:
                await DbManager().rss_delete(int(data[2]))
            await updateRssMenu(query)
        elif data[1].endswith("pause"):
            async with rss_dict_lock:
                for title in list(rss_dict[int(data[2])].keys()):
                    rss_dict[int(data[2])][title]["paused"] = True
            if DATABASE_URL:
                await DbManager().rss_update(int(data[2]))
        elif data[1].endswith("resume"):
            async with rss_dict_lock:
                for title in list(rss_dict[int(data[2])].keys()):
                    rss_dict[int(data[2])][title]["paused"] = False
            if scheduler.state == 2:
                scheduler.resume()
            if DATABASE_URL:
                await DbManager().rss_update(int(data[2]))
        await updateRssMenu(query)
    elif data[1].startswith("all"):
        if len(rss_dict) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
            return
        await query.answer()
        if data[1].endswith("unsub"):
            async with rss_dict_lock:
                rss_dict.clear()
            if DATABASE_URL:
                await DbManager().trunc_table("rss")
            await updateRssMenu(query)
        elif data[1].endswith("pause"):
            async with rss_dict_lock:
                for user in list(rss_dict.keys()):
                    for title in list(rss_dict[user].keys()):
                        rss_dict[int(data[2])][title]["paused"] = True
            if scheduler.running:
                scheduler.pause()
            if DATABASE_URL:
                await DbManager().rss_update_all()
        elif data[1].endswith("resume"):
            async with rss_dict_lock:
                for user in list(rss_dict.keys()):
                    for title in list(rss_dict[user].keys()):
                        rss_dict[int(data[2])][title]["paused"] = False
            if scheduler.state == 2:
                scheduler.resume()
            elif not scheduler.running:
                addJob()
                scheduler.start()
            if DATABASE_URL:
                await DbManager().rss_update_all()
    elif data[1] == "deluser":
        if len(rss_dict) == 0:
            await query.answer(text="No subscriptions!", show_alert=True)
        else:
            await query.answer()
            buttons = ButtonMaker()
            buttons.ibutton("Back", f"rss back {user_id}")
            buttons.ibutton("Close", f"rss close {user_id}")
            button = buttons.build_menu(2)
            msg = "Send one or more user_id separated by space to delete their resources.\nTimeout: 60 sec."
            await editMessage(message, msg, button)
            pfunc = partial(rssDelete, pre_event=query)
            await event_handler(client, query, pfunc)
    elif data[1] == "listall":
        if not rss_dict:
            await query.answer(text="No subscriptions!", show_alert=True)
        else:
            await query.answer()
            start = int(data[3])
            await rssList(query, start, all_users=True)
    elif data[1] == "shutdown":
        if scheduler.running:
            await query.answer()
            scheduler.shutdown(wait=False)
            await sleep(0.5)
            await updateRssMenu(query)
        else:
            await query.answer(text="Already Stopped!", show_alert=True)
    elif data[1] == "start":
        if not scheduler.running:
            await query.answer()
            addJob()
            scheduler.start()
            await updateRssMenu(query)
        else:
            await query.answer(text="Already Running!", show_alert=True)


async def rssMonitor():
    if not config_dict["RSS_CHAT_ID"]:
        scheduler.shutdown(wait=False)
        return
    if len(rss_dict) == 0:
        scheduler.pause()
        return
    all_paused = True
    for user, items in list(rss_dict.items()):
        for title, data in list(items.items()):
            try:
                if data["paused"]:
                    continue
                tries = 0
                while True:
                    try:
                        async with ClientSession() as session:
                            async with session.get(data["link"], ssl=False) as res:
                                html = await res.text()
                        break
                    except:
                        tries += 1
                        if tries > 3:
                            raise
                        continue
                rss_d = feedparse(html)
                try:
                    last_link = rss_d.entries[0]["links"][1]["href"]
                except IndexError:
                    last_link = rss_d.entries[0]["link"]
                finally:
                    all_paused = False
                last_title = rss_d.entries[0]["title"]
                if data["last_feed"] == last_link or data["last_title"] == last_title:
                    continue
                feed_count = 0
                while True:
                    try:
                        await sleep(10)
                    except:
                        raise RssShutdownException("Rss Monitor Stopped!")
                    try:
                        item_title = rss_d.entries[feed_count]["title"]
                        item_info = rss_d.entries[feed_count]["summary"]
                        try:
                            url = rss_d.entries[feed_count]["links"][1]["href"]
                        except IndexError:
                            url = rss_d.entries[feed_count]["link"]
                        if data["last_feed"] == url or data["last_title"] == item_title:
                            break
                    except IndexError:
                        LOGGER.warning(
                            f"Reached Max index no. {feed_count} for this feed: {title}. Maybe you need to use less RSS_DELAY to not miss some torrents"
                        )
                        break
                    parse = True
                    for flist in data["exf"]:
                        if any(x in item_title for x in flist):
                            parse = False
                            feed_count += 1
                            break
                    if not parse:
                        continue
                    for flist in data["exf"]:
                        if any(x in item_info for x in flist):
                            parse = False
                            feed_count += 1
                            break
                    if not parse:
                        continue
                    for flist in data["inf"]:
                        if all(x not in item_title for x in flist):
                            parse = False
                            feed_count += 1
                            break
                    if not parse:
                        continue
                    if command := data["command"]:
                        cmd = command.split(maxsplit=1)
                        cmd.insert(1, url)
                        feed_msg = " ".join(cmd)
                        if not feed_msg.startswith("/"):
                            feed_msg = f"/{feed_msg}"
                    else:
                        feed_msg = f"<b>{item_title.replace('>', '').replace('<', '').replace('.', ' ')}</b>"
                        item_info = re_sub(r'#\d{7} \|.*?\|', '', item_info)
                        item = {
                            'IMDB Rating: ': '\n<b>IMDB Rating: </b>',
                            'Genre: ': '\n<b>Genre: </b>',
                            'Size: ': '\n<b>Size: </b>',
                            'Type: ': '\n<b>Type: </b>',
                            'Lang: ': '\n<b>Lang: </b>',
                            'Runtime: ': '\n<b>Runtime: </b>',
                            ' min': ' min\n<b>Desc: </b>',
                            '—': '\n<b>By: </b>',
                            'Added: ': '\n<b>Added: </b>',
                            '+0000': ''
                        }
                        for old, new in item.items():
                            item_info = item_info.replace(old, new)
                        feed_msg += f"\n\n<b>Info: </b>{item_info}"
                        feed_msg += f"\n\n<b>Download Link: </b>{url}"
                        feed_msg += "\n\n<b>Powered By: </b>@Z_Mirror"
                    await sendRss(feed_msg)
                    feed_count += 1
                async with rss_dict_lock:
                    if user not in rss_dict or not rss_dict[user].get(title, False):
                        continue
                    rss_dict[user][title].update(
                        {"last_feed": last_link, "last_title": last_title}
                    )
                await DbManager().rss_update(user)
                LOGGER.info(f"Feed Name: {title} -Feed Link: {last_link}")
            except Exception as e:
                LOGGER.error(str(e))
                continue
    if all_paused:
        scheduler.pause()


async def rssList(query, start, all_users=False):
    buttons = ButtonMaker()
    buttons.ibutton("Back", f"rss back {query.from_user.id}")
    buttons.ibutton("Close", f"rss close {query.from_user.id}")
    button = buttons.build_menu(2)
    msg = ""
    count = 0
    if all_users:
        rss_list = rss_dict
    else:
        rss_list = {query.from_user.id: rss_dict.get(query.from_user.id, {})}
    for user, items in list(rss_list.items()):
        msg += f"<b>User:</b> <code>{user}</code>\n\n"
        if len(items) == 0:
            msg += "No subscriptions!\n\n"
        for title, data in items.items():
            if count < start:
                count += 1
                continue
            if count == start + 5:
                break
            status = "Paused" if data["paused"] else "Active"
            msg += f"<b>Title:</b> <code>{title}</code>\n"
            msg += f"<b>Status:</b> <code>{status}</code>\n\n"
            count += 1
    if count == 0:
        msg += "No subscriptions!"
    await editMessage(query.message, msg, button)


async def updateRssMenu(query):
    await editMessage(query.message, *await rssMenu(query.message))


def addJob():
    if not scheduler.running:
        scheduler.add_job(
            rssMonitor,
            trigger=IntervalTrigger(minutes=int(config_dict["RSS_DELAY"])),
            id="rss_listener",
            name="RSS Feed Listener",
            replace_existing=True,
        )


if scheduler.running:
    addJob()



   
