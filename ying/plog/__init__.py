import subprocess
import os
import pty
import sys
from datetime import datetime
import re
from ying.thirdparty.telegram import send_message, edit_message_text
from ying.utils.runtime import format_host_banner
from ying.config import settings

ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def send_data_to_telegram(data, message_id):
    chat_id = settings.get("tg_chat_id", None)
    if chat_id is None:
        raise ValueError("tg_chat_id is not configured, please add it.")

    if message_id is None:
        message_id = send_message(chat_id, data)
    edit_message_text(chat_id, message_id, data)
    return message_id


def run_command(command):
    master_fd, slave_fd = pty.openpty()

    process = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )

    os.close(slave_fd)

    host_banner = format_host_banner()

    telegram_message_id = None
    while True:
        try:
            output = os.read(master_fd, 1024)
            if not output:
                break
            sys.stdout.buffer.write(output)
            sys.stdout.buffer.flush()
            filtered_output = ansi_escape.sub("", output.decode("utf-8"))

            # send to telegram
            text = f'`{datetime.now().strftime("[%H:%M:%S]")} {host_banner}`\n'
            text += f"$ `{command}`\n```\n{filtered_output.lstrip()}```"
            telegram_message_id = send_data_to_telegram(text, telegram_message_id)

        except OSError:
            break

    process.wait()
