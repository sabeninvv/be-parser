from typing import Set
from os import getenv
from aiohttp import ClientSession


class TelegramClient:
    def __init__(self, session: ClientSession, **kwargs):
        self.__path = f"https://api.telegram.org/bot{kwargs.get('token')}"
        self.__session = session
        self.__chats_ids = set()

    async def get_chat_ids(self, default_chat_id: int) -> Set[int]:
        async with self.__session.get(url=f"{self.__path}/getUpdates") as response:
            response.raise_for_status()
            response = await response.json()
            seq_msgs = response['result']
            if not seq_msgs:
                return {default_chat_id}
            chat_ids = [msg.get('message', {}).get('chat', {}).get('id', None) for msg in seq_msgs]
            self.__chats_ids |= set(_id for _id in chat_ids if _id is not None)
            return self.__chats_ids

    async def send_msg2chat(self, chat_id: int, msg: str):
        return await self.__session.post(url=f"{self.__path}/sendMessage",
                                         params={'chat_id': chat_id, 'text': msg})

    async def send_msg2all_chats(self, msg: str):
        default_chat_id = int(getenv('TELEGRAM_PHONE4PUSH'))
        _ = [await self.send_msg2chat(chat_id=chat_id, msg=msg) for chat_id in await self.get_chat_ids(default_chat_id)]
