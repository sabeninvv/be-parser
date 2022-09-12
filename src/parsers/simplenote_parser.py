from os import getenv
from typing import List, Set
from multiprocessing import Queue
from time import time, sleep

import requests
from bs4 import BeautifulSoup
from redis import StrictRedis
from ..logger import logger
from ..tools import decoding_bytes2str, retry
from ..dataclasses import Match


class SimplenoteParser:
    def __init__(self, queue: Queue):
        self._name = "SimplenoteParser"
        self.__last_ts = 0
        self.__inmonitoring_matches = set()
        self.__endmonitoring_matches = set()
        self.__connection = StrictRedis(password=getenv('REDIS_PASSWORD'))
        self.__queue = queue

    def parse_request(self, response: str) -> Match:
        uri, *bookmakers = response.strip().split(",")
        bookmakers = ['10bet', '1xbet', 'bet-at-home', 'betvictor', 'betway',
                      'bwin', 'comeon', 'pinnacle', 'unibet', 'william hill',
                      'youwin', 'betfair exchange'] if not bookmakers else bookmakers
        bookmakers = set(bookmaker.lower() for bookmaker in bookmakers)
        match_meta = Match(uri=uri, bookmakers=bookmakers)
        return match_meta

    def get_html_from_simplenote(self, uri: str) -> bytes:
        return requests.get(url=uri).content

    def parse_html(self, html: bytes) -> List[str]:
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('div', {'class': "note note-detail-markdown"})
        return [url.text for url in table.findAll('a')]

    def get_requests(self) -> List[str]:
        return self.parse_html(html=self.get_html_from_simplenote(uri=getenv('URI_SIMPLENOTE')))

    @property
    def inmonitoring_matches(self) -> Set[str]:
        return decoding_bytes2str(self.__connection.smembers("match.inmonitoring")) | self.endmonitoring_matches | self.inflight_matches

    @property
    def endmonitoring_matches(self) -> Set[str]:
        self.__endmonitoring_matches: List[bytes] = self.__connection.smembers("match.endmonitoring")
        return decoding_bytes2str(self.__endmonitoring_matches)

    @property
    def inflight_matches(self) -> Set[str]:
        self.__inflight_matches: List[bytes] = self.__connection.smembers("match.inflight")
        return decoding_bytes2str(self.__inflight_matches)

    @retry(logger=logger, num_tries=float("inf"), idle_time_sec=3.)
    def fill_redis_with_matches(self):
        """
        Получить данные от клиента
        Распарсить данные
        Получить данные из inmonitoring_matches
        Выявить данные которых нету в inmonitoring_matches
        Записать данные в inmonitoring
        :return:
        """
        while True:
            rows_from_evernote = self.get_requests()
            if not rows_from_evernote:
                sleep(60.)
                continue

            wanted_matches: List[Match] = [self.parse_request(request) for request in set(rows_from_evernote)]
            hashes_wanted_matches: List[str] = [match.hash for match in wanted_matches]
            inxs2del = [hashes_wanted_matches.index(_hash) for _hash in self.inmonitoring_matches
                        if _hash in hashes_wanted_matches]
            wanted_matches = [match for inx, match in enumerate(wanted_matches) if inx not in inxs2del]
            hashes_wanted_matches = [match for inx, match in enumerate(hashes_wanted_matches) if inx not in inxs2del]

            if not wanted_matches:
                sleep(60.)
                continue

            for match in wanted_matches:
                _ = self.__connection.sadd(f"{match.hash}:bookmakers", *match.bookmakers)
                _ = self.__connection.set(f"{match.hash}:timestamp", int(time()))
                self.__queue.put(match.hash)
                logger.info(f"[{self._name}] put2queue {match.uri}, {match.bookmakers}")

            _ = self.__connection.sadd("match.inmonitoring", *hashes_wanted_matches)
            sleep(60.)