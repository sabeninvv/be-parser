from os import getenv
from random import uniform
from typing import Optional, List, Set, Any, Dict
from multiprocessing import Queue
from queue import Empty
from time import time

import asyncio
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_socks import ProxyType, ProxyConnector
from bs4 import BeautifulSoup
from redis import StrictRedis
from ..logger import logger
from ..tools import decode_by_b64, decoding_bytes2str, retry_async
from ..service import TelegramClient


class BetExplorerParser:
    def __init__(self, queue: Queue):
        self._name = "BetExplorerParser"
        self.__futures = set()
        self.num_semaphore_limits = 1_000
        self.queue = queue
        self.__connection = StrictRedis(password=getenv('REDIS_PASSWORD'))
        self.__telegram_cli: Optional[TelegramClient] = None

    @retry_async(logger=logger, num_tries=200, idle_time_sec=.5)
    async def get_html_match_info(self, uri: str) -> Dict[str, Any]:
        tail = -2 if uri[-1] == "/" else -1
        match_id = uri.split("/")[tail]

        cookies = {'my_timezone': '%2B1'}

        headers = {
            'authority': 'www.betexplorer.com',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': uri,
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }
        async with self.__session.get(url=f'https://www.betexplorer.com/match-odds/{match_id}/1/1x2/',
                                      cookies=cookies, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.json(content_type="text/plain")

    @retry_async(logger=logger, num_tries=200, idle_time_sec=.5)
    async def get_bookmakers_from_match(self, uri):
        await asyncio.sleep(uniform(.1, 1.1))
        html_match_info = await self.get_html_match_info(uri=uri)
        soup = BeautifulSoup(html_match_info["odds"], 'lxml')
        blocks = soup.findAll('td', {'class': "h-text-left over-s-only"})
        return [block.text.lower() for block in blocks]

    async def wrap_scrap(self, match_hash):
        try:
            return await self.scraping(match_hash=match_hash)
        except Exception as ex:
            logger.error(f"[{self._name}] {ex}", exc_info=True)
            return None, match_hash

    async def scraping(self, match_hash: str):
        if self.__connection.sismember("match.inflight", match_hash) == 1:
            return None, match_hash

        with self.__connection.pipeline() as pipe:
            pipe.sadd("match.inflight", match_hash)
            pipe.smembers(f"{match_hash}:bookmakers")
            _, wanted_bookmakers_all = pipe.execute()

        if not wanted_bookmakers_all:
            _ = self.__connection.srem("match.inflight", match_hash)
            return None, match_hash

        wanted_bookmakers_all: List[str] = list(decoding_bytes2str(wanted_bookmakers_all))

        with self.__connection.pipeline() as pipe:
            _ = [pipe.get(f"{match_hash}:bookmakers:{bookmaker}") for bookmaker in wanted_bookmakers_all]
            response = pipe.execute()

        endmonitoring_bookmakers = set(wanted_bookmakers_all[inx] for inx, val in enumerate(response) if val is not None)
        wanted_bookmakers: Set[str] = set(wanted_bookmakers_all) - endmonitoring_bookmakers
        logger.info(f"[{self._name}] {decode_by_b64(match_hash).split('|')[0]} wanted_bookmakers: {wanted_bookmakers}")

        uri, *_ = decode_by_b64(match_hash).split("|")
        while wanted_bookmakers:
            await asyncio.sleep(5.)
            if time() - int(self.__connection.get(f"{match_hash}:timestamp")) > 7 * 24 * 60 * 60:
                logger.info(f"[{self._name}] get rotten {uri}")
                break

            inlive_bookmakers: List[str] = await self.get_bookmakers_from_match(uri=uri)
            founded_bookmakers = [wanted_bookmaker for wanted_bookmaker in wanted_bookmakers
                                  if wanted_bookmaker in inlive_bookmakers]
            if not founded_bookmakers:
                continue

            with self.__connection.pipeline() as pipe:
                _ = [pipe.set(f"{match_hash}:bookmakers:{founded_bookmaker}", b"1")
                     for founded_bookmaker in founded_bookmakers]
                _ = pipe.execute()

            for founded_bookmaker in founded_bookmakers:
                wanted_bookmakers.discard(founded_bookmaker)
                match_name = " => ".join(uri.split("/")[3:-2 if uri[-1] == "/" else -1])
                logger.info(f"[{self._name}] {match_name} | found: {founded_bookmaker}")
                await self.__telegram_cli.send_msg2all_chats(msg=f"{match_name} | found: {founded_bookmaker}")
                await asyncio.sleep(.00001)

        return wanted_bookmakers_all, match_hash

    def oncallback(self, future):
        wanted_bookmakers_all, match_hash = future.result()
        _ = self.__connection.srem("match.inflight", match_hash)
        if wanted_bookmakers_all is not None:
            _ = self.__connection.sadd("match.endmonitoring", match_hash)
            _ = self.__connection.srem("match.inmonitoring", match_hash)
            keys4del = [f"{match_hash}:bookmakers:{bookmaker}" for bookmaker in wanted_bookmakers_all]
            keys4del = [*keys4del, *wanted_bookmakers_all, f"{match_hash}:timestamp", f"{match_hash}:bookmakers"]
            _ = self.__connection.delete(*keys4del)
        self.__futures.discard(future)
        self.create_future(match_hash=match_hash)

    def create_future(self, match_hash: str):
        future: asyncio.Future = asyncio.ensure_future(coro_or_future=self.wrap_scrap(match_hash=match_hash))
        future.add_done_callback(self.oncallback)
        self.__futures.add(future)

    async def get_data(self):
        while True:
            try:
                data = self.queue.get(timeout=.001)
                if data is not None:
                    return data
            except Empty:
                ...
            await asyncio.sleep(.001)

    async def ride_round_robin(self):
        connector = ProxyConnector(proxy_type=ProxyType.SOCKS5, host='127.0.0.1', port=9050, rdns=True)
        # connector = TCPConnector(force_close=True)
        self.__session: ClientSession = ClientSession(connector=connector, trust_env=True,
                                                      timeout=ClientTimeout(total=60 * 60, sock_read=480))
        self.__telegram_cli = TelegramClient(session=self.__session,
                                             token=getenv('TELEGRAM_TOKEN'))
        while True:
            while len(self.__futures) > self.num_semaphore_limits:
                await asyncio.sleep(.001)
            match_hash: str = await self.get_data()
            self.create_future(match_hash=match_hash)
