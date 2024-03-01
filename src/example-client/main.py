#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import difflib
import json

from shared.client import AsyncClient, ProvisioningMessage
from shared.client.config import settings


def _cprint(text: str, fg: str = None, bg: str = None, **kwargs):
    colors = ["k", "r", "g", "y", "b", "v", "c", "w"]

    def fg_color(color):
        return str(30 + colors.index(color))

    def bg_color(color):
        return str(40 + colors.index(color))

    if fg:
        color = f"0;{fg_color(fg)}"
        if bg:
            color += f";{bg_color(bg)}"
        print("\x1b[6" + color + "m" + text + "\x1b[0m", **kwargs)
    else:
        print(text, **kwargs)


def print_header(msg: ProvisioningMessage, action=None):
    print()

    text = ""
    if action:
        text = f"Action: {action}  "
    else:
        text = f"Realm: {msg.realm}  "
    _cprint(
        text + f"##  Topic: {msg.topic}  "
        f"##  From: {msg.publisher_name}  "
        f"##  Time: {msg.ts}",
        fg="k",
        bg="w",
    )


def print_object(obj: dict, prefix="", **kwargs):
    lines = json.dumps(obj, indent=2).splitlines()
    for line in lines:
        _cprint(f"{prefix}{line}", **kwargs)


def print_udm_diff(old: dict, new: dict):
    olds = json.dumps(old, indent=2, sort_keys=True).splitlines()
    news = json.dumps(new, indent=2, sort_keys=True).splitlines()
    diff = difflib.unified_diff(olds, news, n=99999)
    # import sys
    # sys.stdout.writelines(diff)
    for line_number, line in enumerate(diff):
        if line_number < 3:
            continue

        if line.startswith("+"):
            _cprint(line, fg="g")
        elif line.startswith("-"):
            _cprint(line, fg="r")
        else:
            _cprint(line)


def handle_udm_message(msg: ProvisioningMessage):
    old_full = msg.body.get("old") or {}
    new_full = msg.body.get("new") or {}

    def shrink(obj, keys):
        return {key: obj.get(key) for key in keys} if obj else {}

    keep_keys = ["dn", "properties", "options", "policies"]
    old = shrink(old_full, keep_keys)
    new = shrink(new_full, keep_keys)

    if old and new:
        print_header(msg, "Object changed")
        print_udm_diff(old, new)
    elif not old:
        print_header(msg, "Object created")
        print_object(new, prefix="+ ", fg="g")
    elif not new:
        print_header(msg, "Object deleted")
        print_object(old, prefix="- ", fg="r")
    else:
        print_header(msg)
        _cprint("No object data received!", fg="r")


def handle_any_message(msg: ProvisioningMessage):
    print_header(msg)
    print(msg.model_dump_json(indent=2))


def handle_message(message: ProvisioningMessage):
    if message.realm == "udm":
        handle_udm_message(message)
    else:
        handle_any_message(message)


async def main():
    name = settings.consumer_name

    client = AsyncClient()

    # TODO: move this call to the AdminClient
    await client.create_subscription(
        name,
        settings.realms_topics,
        settings.provisioning_api_password,
        settings.request_prefill,
    )

    while True:
        response = await client.get_subscription_messages(name=name, timeout=5)
        for message in response:
            handle_message(message=message)

    # FIXME: No stream available any more. Where did it go?
    # async with client.stream(name) as stream:
    #     while True:
    #         # handle incoming message
    #         message: shared.client.Message = await stream.receive_message()
    #         if message.realm == "udm":
    #             handle_udm_message(message)
    #         else:
    #             handle_any_message(message)

    #         # confirm message reception
    #         status = shared.client.MessageProcessingStatus.ok
    #         await stream.send_report(status)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
