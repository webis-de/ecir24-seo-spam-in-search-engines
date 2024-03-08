#!/usr/bin/env python3
#
# Copyright 2021 Janek Bevendorff
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from base64 import b64decode, urlsafe_b64decode
from collections import Counter
import gzip
import io
import json
import logging
from pathlib import Path
import os
from random import choice, random, randint
import re
import socket
import statistics
import string
import subprocess
import sys
import time
from urllib import parse as urlparse
import uuid

import apache_beam as beam
from fastwarc import warc
from apache_beam.transforms import pvalue
import apache_beam.typehints as t
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
import chatnoir_api
from chatnoir_api.v1 import search as chatnoir_search
import click
import httpx
from resiliparse.beam.textio import ReadFromText
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
from tqdm import tqdm

try:
    import jinja2
    from youtube_transcript_api import CouldNotRetrieveTranscript, TooManyRequests, YouTubeTranscriptApi
except ModuleNotFoundError:
    pass

from serp_crawler.config import get_config


logger = logging.getLogger()
PATH = os.path.dirname(__file__)


@click.group()
def main():
    pass


def get_pipeline_opts(**override_args):
    """Construct PipelineOpts object from config with override arguments."""
    cfg = get_config()['pipeline_opts'].copy()
    cfg.update(override_args)
    return PipelineOptions(**cfg)


class BinaryFileSink(fileio.TextSink):
    """
    FileSink that writes binary data.
    """
    def write(self, record):
        if isinstance(record, str):
            record = record.encode('utf8')
        self._fh.write(record)


# List of user agent strings (has to be rotated regularly to circumvent blacklisting).
USER_AGENTS = [
    'Mozilla/5.0 (X11; CrOS x86_64 15999.99.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5754.1 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 10; LIFETAB E1080X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; LaTabStandRB) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 10; TEOX103) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; S22_EEA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla / 5.0(Windows NT 10.0; WOW64; rv: 52.0) Gecko / 20100101 Firefox / 52.0',
    'Mozilla 5.0 (Windows NT 10.0; Win32; x86; rv:88.0) Gecko/20100101 Firefox/88.0.1',
    'Mozilla 5.0 (Windows NT 10.0; Win32; x86; rv;88.0) Gecko/20100101 Firefox/88.0',
    'Mozilla/5.0 (X11; Linux x86_64; Chromium GOST) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; U; Linux x86_64; rv:116.0esr) Gecko/20171214 Firefox/116.0esr',
    'Mozilla/5.0 (X11; Linux x86_64; Quest Pro) AppleWebKit/537.36 (KHTML, like Gecko) OculusBrowser/27.1.0.11.62.475067835 SamsungBrowser/4.0 Chrome/112.0.5615.179 VR Safari/537.36',
    'Mozilla/5.0 (X11; U; Linux i686; rv:123.0esr) Gecko/20112401 Firefox/123.0esr',
    'Mozilla/5.0 (X11; Linux x86_64) Gecko/20011604 Firefox/120.0',
    'Mozilla/5.0 (X11; Linux x86_64) Gecko/20070914 Firefox/118.0',
    'Mozilla/5.0 (X11; Linux i686; en-US) Gecko/20002104 Firefox/122.0esr',
    'Mozilla/5.0 (Windows NT 10.0; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Windows NT 6.3; rv:102.0) Gecko/20100101 Goanna/6.2 Firefox/102.0 PaleMoon/32.2.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:200.0) Gecko/20100101 Firefox/200.0 f645f118e1ed5824f57251e898d8c3a2+',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; x64; rv:115.0esr) Gecko/20100101 Firefox/115.0esr',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_4_1; rv:116.0) Gecko/20110101 Firefox/116.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2; rv:121.0) Gecko/20000101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_13; rv:115.0) Gecko/20110101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1_2; rv:123.0esr) Gecko/20010101 Firefox/123.0esr',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 OPR/99.0.4788.5',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 OPR/99.0.0.0 (Edition Yx 05),'
    'Mozilla 5.0 (Linux; U; Android 13) Chrome/104.0.5112.99',
    'Mozilla/5.0 (Android 13; Mobile; rv:101.0) Gecko/101.0 Firefox/101.0',
    'Mozilla/5.0 (Linux; Android 13; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; Pixel 4a) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.83 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; Pixel 6a) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.95 AtContent/98.5.2194.95',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.100',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 EdgA/109.0.1518.53',
    'Mozilla/5.0 (Linux; Android 13; RMX3521) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 EdgA/109.0.1518.70',
    'Mozilla/5.0 (Linux; Android 12; Redmi Note 9S) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 EdgA/109.0.0.0',
    'Mozilla/5.0 (Linux; Android 12; Infinix X6815B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 EdgA/109.0.1518.80',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/109.0.1518.80 Version/16.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/109.0.1518.80 Version/16.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; SM-A226B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 EdgA/109.0.1518.53',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.49',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1474.0',
    'Mozilla/5.0 (iPad; CPU Ipad OS 14_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148',
    'Mozilla/5.0 (iPad; CPU iPhone OS 14_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/12.0.0 Mobile/15A5370a Safari/602.1',
    'Mozilla/5.0 (X11; CrOS x86_64 15393.12.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS aarch64 15359.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.62 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS aarch64 15359.58.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.134 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS aarch64 15269.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS x86_64 15269.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS x86_64 15178.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS aarch64 15178.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS x86_64 15075.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; CrOS aarch64 15075.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
]


SUPPORTED_SEARCH_ENGINES = ['ddg', 'startpage', 'bing', 'chatnoir']


def request_serp(search_engine, query, user_agent_string=None, backoff=1, max_backoff=64):
    """
    Issue HTTP request for a search result page from the specified search engine using a random user agent string.

    :param search_engine: search engine name (either ``'ddg``, ``'startpage'``, ``'bing'``, or ``'chatnoir'``)
    :param query: search query
    :param user_agent_string: set fixed UA string instead of choosing one at random
    :param backoff: exponential backoff in case of failure (exponent base 2)
    :param max_backoff: maximum backoff in seconds
    :return: structured list of search results
    """

    wait = 0
    while True:
        if search_engine == 'ddg':
            result_list = request_ddg_serp(query, user_agent_string)
        elif search_engine == 'startpage':
            result_list = request_startpage_serp(query, user_agent_string)
        elif search_engine == 'bing':
            result_list = request_bing_serp(query, user_agent_string)
        elif search_engine == 'chatnoir':
            result_list = request_chatnoir_serp(query)
        else:
            raise ValueError(f'Illegal search engine name: {search_engine}')

        if result_list:
            return result_list

        wait += backoff
        b = 2 ** wait
        if b > max_backoff:
            return []
        logger.info('Result list empty, retrying in %i seconds...', b)
        time.sleep(b)


def request_chatnoir_serp(query):
    """
    Issue HTTP request for a search result page from ChatNoir (ClueWeb22).

    :param query: search query
    :return: structured list of search results
    """

    try:
        results = chatnoir_search(get_config()['chatnoir_apikey'], query, page_size=30,
                                  retries=30, staging=True, index=chatnoir_api.Index.ClueWeb22)[:30]
        return [
            dict(title=r.title.text, url=r.target_uri, snippet=r.snippet.text) for r in results
        ]
    except Exception as e:
        logger.error('Connection error while fetching results.')
        logger.exception(e)
        return []


def request_ddg_serp(query, user_agent_string=None):
    """
    Issue HTTP request for a search result page from DuckDuckGo using a random user agent string.

    :param query: search query
    :param user_agent_string: set fixed UA string instead of choosing one at random
    :return: structured list of search results
    """

    request_headers = {
        'User-Agent': (user_agent_string or choice(USER_AGENTS)),
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.8',
        'Accept-Encoding': 'gzip,deflate',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://html.duckduckgo.com/',
        'Origin': 'https://html.duckduckgo.com'
    }

    try:
        resp = httpx.post('https://html.duckduckgo.com/html/',
                          content=urlparse.urlencode(dict(q=query, b='', kl='', df='')).encode(),
                          headers=request_headers)
        response_bytes = resp.read()

        if not response_bytes:
            logger.error('Invalid server response')
            return []

        tree = HTMLTree.parse_from_bytes(response_bytes, 'utf-8')
        result_list = []
        for qr in tree.body.query_selector_all('#links .result'):
            result_a = qr.query_selector('.result__title .result__a')
            if not result_a:
                logger.error('Result has no title link.')
                continue
            result_url = result_a['href']
            if result_url.startswith('//'):
                result_url = urlparse.parse_qs(urlparse.urlparse(result_url).query)['uddg'][0]
            snippet = qr.query_selector('.result__snippet')
            result_list.append(dict(
                title=result_a.text.strip(),
                url=result_url,
                snippet=snippet.text.strip() if snippet else '',
            ))
        return result_list
    except Exception as e:
        logger.error('Connection error while fetching results.')
        logger.exception(e)
        return []


def request_bing_serp(query, user_agent_string=None):
    """
    Issue HTTP request for a search result page from Bing using a random user agent string.

    :param query: search query
    :param user_agent_string: set fixed UA string instead of choosing one at random
    :return: structured list of search results
    """

    qs_encoded = urlparse.urlencode(dict(q=query, go='Search', qs='n', form='QBRE'))
    request_headers = {
        'User-Agent': user_agent_string or ''.join(choice(
            ' ' + string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(randint(10, 100))),
        'Accept': 'text/html,application/xhtml+xml',
        # 'Referer:': f'https://www.bing.com/?{qs_encoded}',
        'Accept-Language': 'en-US,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Cookie': 'SRCHHPGUSR=SRCHLANG=en&PV=5.15.0&BRW=XW&BRH=T&CW=1784&CH=1163&SCW=1769&SCH=6179&DPR=1.0&UTC=60&DM=0&'
                  'HV=1668169678&WTS=63803766137&PRVCW=1784&PRVCH=1163&NEWWND=0&NRSLT=30&LSL=0&AS=1&ADLT=DEMOTE&NNT=1&'
                  'HAP=0&VSRO=1'
    }

    try:
        resp = httpx.get(f'https://www.bing.com/search?{qs_encoded}', headers=request_headers)
        response_bytes = resp.read()

        if not response_bytes:
            logger.error('Invalid server response')
            return []

        tree = HTMLTree.parse_from_bytes(response_bytes, 'utf-8')
        result_list = []

        for qr in tree.body.query_selector_all('#b_results > li.b_algo'):
            result_a = qr.query_selector('h2 a[h^="ID=SERP"], .b_algoheader > a[h^="ID=SERP"]')
            if not result_a:
                logger.error('Result has no title link.')
                continue
            attrib = result_a.query_selector('.b_attribution')
            if attrib:
                attrib.decompose()

            result_url = result_a['href']
            if result_url.startswith('https://www.bing.com/ck/a?!'):
                # De-obfuscate URLs
                result_url_qs = urlparse.parse_qs(urlparse.urlparse(result_url).query)
                result_url = urlsafe_b64decode(result_url_qs['u'][0][2:] + '==').decode()
            result_url = result_url.split('#:~:text=', 1)[0]

            snippet = qr.query_selector('.b_lineclamp2, .b_lineclamp3, .b_snippetBigText, .b_paractl')
            if snippet:
                news_dt = snippet.query_selector('.news_dt')
                if news_dt:
                    news_dt.decompose()
            result_list.append(dict(
                title=result_a.text.strip(),
                url=result_url,
                snippet=snippet.text.lstrip().lstrip('·').strip() if snippet else '',
            ))
        return result_list
    except Exception as e:
        logger.error('Connection error while fetching results.')
        logger.exception(e)
        return []


def request_startpage_serp(query, user_agent_string=None):
    """
    Issue HTTP request for a search result page from StartPage using a random user agent string.

    :param query: search query
    :param user_agent_string: set fixed UA string instead of choosing one at random
    :return: structured list of search results
    """

    request_headers = {
        'User-Agent': (user_agent_string or choice(USER_AGENTS)),
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.8',
        'Accept-Encoding': 'gzip,deflate',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://www.startpage.com/',
        'Origin': 'https://www.startpage.com',
        'Cookie': 'preferences=date_timeEEEworldN1Ndisable_family_filterEEE0N1Ndisable_open_in_new_windowEEE1N1N'
                  'enable_post_methodEEE1N1Nenable_proxy_safety_suggestEEE1N1Nenable_stay_controlEEE0N1Ninstant_answers'
                  'EEE0N1Nlang_homepageEEEs%2Fdevice%2FenN1NlanguageEEEenglishN1Nlanguage_uiEEEenglishN1Nnum_of_results'
                  'EEE20N1Nsearch_results_regionEEEallN1NsuggestionsEEE0N1Nwt_unitEEEcelsius',
    }

    try:
        resp = httpx.post('https://www.startpage.com/sp/search',
                          content=urlparse.urlencode(dict(
                              query=query,
                              t='device',
                              lui='english',
                              cat='web'
                          )).encode(),
                          headers=request_headers)
        response_bytes = resp.read()

        if not response_bytes:
            logger.error('Invalid server response')
            return []

        tree = HTMLTree.parse_from_bytes(response_bytes, 'utf-8')
        result_list = []
        for qr in tree.body.query_selector_all('.w-gl .w-gl__result'):
            result_a = qr.query_selector('.w-gl__result-title')
            if not result_a:
                logger.error('Result has no title link.')
                continue
            result_url = result_a['href']
            snippet = qr.query_selector('.w-gl__description')
            result_list.append(dict(
                title=result_a.text.strip(),
                url=result_url,
                snippet=snippet.text.strip() if snippet else '',
            ))
        return result_list
    except Exception as e:
        logger.error('Connection error while fetching results.')
        logger.exception(e)
        return []


@main.command()
@click.option('-e', '--search-engine', type=click.Choice(SUPPORTED_SEARCH_ENGINES), default='ddg', show_default=True,
              help='Search engine to scrape')
def verify_user_agents(search_engine):
    """
    Test built-in list of user agent strings for blacklisting and print entries that return results.
    """

    for ua in USER_AGENTS:
        try:
            result = request_serp(search_engine, 'best fax machines', ua)
            if len(result) > 0:
                print(ua)
        finally:
            time.sleep(1.0)


class CrawlSERPs(beam.DoFn):
    """Crawl DuckDuckGo search results."""

    def __init__(self, search_engine, sleep_time, sleep_variance):
        super().__init__()
        self.sleep_time = sleep_time
        self.sleep_variance = sleep_variance
        self.search_engine = search_engine

    def start_bundle(self):
        # Initial sleep to prevent all workers from starting at the same time
        time.sleep(self.sleep_time + random() * self.sleep_variance - self.sleep_variance / 2)

    # noinspection PyMethodOverriding
    def process(self, element: str) -> t.Iterable[t.Dict[str, t.Any]]:
        element = element.strip()
        logger.debug('Querying %s', element)

        try:
            result_list = request_serp(self.search_engine, element)
            out_dict = dict(query=element, results=result_list)
            if not result_list:
                logger.error('Empty result list for query %s (probably rate limiting)', element)
                yield pvalue.TaggedOutput('empty', out_dict)
            else:
                yield pvalue.TaggedOutput('non_empty', out_dict)

        finally:
            time.sleep(self.sleep_time + random() * self.sleep_variance - self.sleep_variance / 2)


class CrawlWebPages(beam.DoFn):
    """Crawl web page HTML from URLs and return compressed WARC records."""

    def __init__(self, sleep_time, sleep_variance):
        super().__init__()
        self.sleep_time = sleep_time
        self.sleep_variance = sleep_variance

    def start_bundle(self):
        # Initial sleep to prevent all workers from starting at the same time
        time.sleep(self.sleep_time + random() * self.sleep_variance - self.sleep_variance / 2)

    # noinspection PyMethodOverriding
    def process(self, element: str) -> t.Iterable[bytes]:

        element = element.strip()
        logger.debug('Querying %s', element)

        request_headers = {
            'User-Agent': choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.8',
            'Accept-Encoding': 'gzip,deflate',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }

        try:
            resp = httpx.get(element, headers=request_headers, follow_redirects=True)

            payload_stream = io.BytesIO()
            payload_stream.write(resp.http_version.encode() + b' ')
            payload_stream.write(str(resp.status_code).encode() + b' ')
            payload_stream.write(resp.reason_phrase.encode() + b'\r\n')

            response_bytes = resp.read()
            content_len = str(len(response_bytes))

            cl_rewritten = False
            for k, v in resp.headers.multi_items():
                if k == 'content-encoding':
                    k = 'x-crawler-content-encoding'
                elif k == 'transfer-encoding':
                    k = 'x-crawler-transfer-encoding'
                elif k == 'content-length' and v != content_len:
                    k = 'x-crawler-content-length'
                    cl_rewritten = True

                payload_stream.write(k.encode('latin1') + b': ' + v.encode('latin1') + b'\r\n')

            if cl_rewritten:
                payload_stream.write(b'content-length: ' + content_len.encode() + b'\r\n')

            payload_stream.write(b'\r\n')
            payload_stream.write(response_bytes)

            record = warc.WarcRecord()
            record.init_headers(record_type=warc.WarcRecordType.response)
            record.headers['Content-Type'] = 'application/http; msgtype=response'
            record.headers['WARC-Target-URI'] = str(resp.url)
            if resp.url != element:
                record.headers['X-WARC-Redirect-From'] = element

            record.set_bytes_content(payload_stream.getvalue())

            out_stream = io.BytesIO()
            record.write(out_stream, True)

            yield gzip.compress(out_stream.getvalue(), compresslevel=9)

        except Exception:
            logger.warning('Failed to fetch URL %s', element)

        finally:
            time.sleep(self.sleep_time + random() * self.sleep_variance - self.sleep_variance / 2)


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('topic-list')
@click.argument('output-dir')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('-e', '--search-engine', type=click.Choice(SUPPORTED_SEARCH_ENGINES), default='startpage',
              show_default=True, help='Search engine to scrape')
@click.option('-s', '--sleep', type=float, default=60.0, show_default=True,
              help='Sleep time between requests in seconds')
@click.option('--sleep-variance', type=float, help='Vary sleep time randomly by seconds',
              default=10.0, show_default=True)
@click.option('-p', '--parallelism', type=int, help='Processing parallelism')
def crawl(topic_list, output_dir, beam_args, search_engine, sleep, sleep_variance, parallelism):
    """
    Crawl DuckDuckGo (Bing) or StartPage (Google) results for a given list of topics.
    """

    sys.argv[1:] = beam_args
    options = get_pipeline_opts(parallelism=parallelism)

    with beam.Pipeline(options=options) as pipeline:
        empty, non_empty = (
            pipeline
            | 'Read Queries' >> ReadFromText(topic_list, shuffle_splits=False, shuffle_names=False)
            | 'Shuffle Queries' >> beam.Reshuffle()
            | 'Retrieve Results' >> beam.ParDo(
                    CrawlSERPs(search_engine, sleep, sleep_variance)).with_outputs('empty', 'non_empty')
        )

        _ = (
            non_empty
            | 'Serialize JSON' >> beam.Map(json.dumps)
            | 'Write JSONL' >> fileio.WriteToFiles(output_dir, file_naming=fileio.default_file_naming(
                                                   'query_results', '.jsonl'))
        )

        __ = (
            empty
            | beam.Map(lambda d: d['query'])
            | 'Log Unanswered Queries' >> fileio.WriteToFiles(output_dir, file_naming=fileio.default_file_naming(
                                                              'queries_unanswered', '.txt'))
        )


@main.command()
@click.argument('result-file', type=click.Path(dir_okay=False, exists=False))
@click.argument('warc-outdir', type=click.Path(file_okay=False, exists=False))
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('-s', '--sleep', type=float, default=10.0, show_default=True,
              help='Sleep time between requests in seconds')
@click.option('--sleep-variance', type=float, help='Vary sleep time randomly by seconds',
              default=5.0, show_default=True)
@click.option('-p', '--parallelism', type=int, help='Processing parallelism')
def crawl_results(result_file, warc_outdir, beam_args, sleep, sleep_variance, parallelism):
    """Perform a shallow crawl of the HTML web pages from a SERP result file."""

    sys.argv[1:] = beam_args
    options = get_pipeline_opts(parallelism=parallelism)

    with beam.Pipeline(options=options) as pipeline:
        _ = (
            pipeline
            | 'Read Result URLs' >> ReadFromText(result_file, shuffle_splits=False, shuffle_names=False)
            | 'Map URLs' >> beam.FlatMap(lambda l: [r['url'] for r in json.loads(l)['results']])
            | 'Shuffle URLs' >> beam.Reshuffle()
            | 'Crawl Web Pages' >> beam.ParDo(CrawlWebPages(sleep, sleep_variance))
            | 'Write WARCs' >> fileio.WriteToFiles(
                    warc_outdir,
                    sink=BinaryFileSink,
                    file_naming=fileio.default_file_naming('results', '.warc.gz'))
        )


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('query')
@click.option('-e', '--search-engine', type=click.Choice(SUPPORTED_SEARCH_ENGINES), default='ddg', show_default=True,
              help='Search engine to scrape')
@click.option('-i', '--indent', type=int, help='Pretty print JSON output with indent size')
@click.option('-b', '--max-backoff', type=int, default=3, help='Maximum backoff exponent between retries')
def query_single(query, search_engine, indent, max_backoff):
    """
    Query a single DuckDuckGo (Bing) or StartPage (Google) results page for a given topic.
    """

    print(json.dumps(dict(
        query=query,
        results=request_serp(search_engine, query, max_backoff=max_backoff)
    ), indent=indent))


def decode_adclick_url(url):
    """Extract target URL from DDG/Bing adblick tracking URL."""
    url = urlparse.parse_qs(urlparse.urlparse(url).query)['u3'][0]
    url = urlparse.parse_qs(urlparse.urlparse(url).query)['u'][0]
    return urlparse.unquote(b64decode(url.encode() + b'==').decode())


@main.command()
@click.argument("search_results_file", type=click.Path(dir_okay=False, exists=True))
@click.argument("target_directory", type=click.Path(file_okay=False, exists=False))
@click.option("-b", "--query-blacklist", help="Skip queries listed in this file",
              type=click.Path(dir_okay=False, exists=True))
@click.option("-p", "--part-size", type=int, default=500, show_default=True, help="Number of directories per part")
@click.option('-c', '--chatnoir-index', type=str,
              help='Rewrite URL to point to the ChatNoir cache for the given index (e.g. cw22)')
def scriptor_prep(search_results_file, target_directory, query_blacklist, part_size, chatnoir_index):
    """Generate Scriptor inputs from scraped search results."""

    search_results_file = Path(search_results_file)

    target_directory = Path(target_directory)
    if not target_directory.exists():
        target_directory.mkdir(exist_ok=True, parents=True)

    if query_blacklist:
        query_blacklist = set(l.strip() for l in open(query_blacklist))

    browser_contexts = target_directory / "browserContexts"
    (browser_contexts / "default").mkdir(exist_ok=True, parents=True)
    open(browser_contexts / "default" / "browser.json", "w").write(json.dumps({
        "viewport": {
            "width": 1280,
            "height": 720
        },
        "headless": False,
        "args": [
            "--disable-extensions-except=/script/cookies",
            "--load-extension=/script/cookies"
        ]
    }))

    for qidx, line in enumerate(tqdm(open(search_results_file, "r"))):
        results = json.loads(line)
        if query_blacklist and results["query"] in query_blacklist:
            continue

        pdir = target_directory / f"part-{qidx // part_size}"
        if not pdir.exists():
            pdir.mkdir(exist_ok=True, parents=True)

        open(pdir / f"query-{qidx}.txt", "w").write(results["query"])
        with open(pdir / f"query-{qidx}-hits.jsonl", "w") as configs_out:
            for result in results["results"]:
                url = result["url"]
                if chatnoir_index:
                    url = 'https://chatnoir-webcontent.web.webis.de/?index={}&raw&url={}'.format(
                        chatnoir_index, urlparse.quote(url))
                configs_out.write(json.dumps({"url": url}) + "\n")


@main.command()
@click.argument('config_path', type=click.Path(exists=True, file_okay=False))
@click.argument('input_path', type=click.Path(exists=True, file_okay=False))
@click.argument('output_path', type=click.Path(file_okay=False))
def scriptor_crawl(config_path, input_path, output_path):
    """
    Perform a rich crawl of the web pages from a SERP result file using Scriptor (requires Kubernetes).

    All paths must be on a shared file system accessible by all Kubernetes pods.
    ``config_path`` must point to a valid Scriptor script/config directory.
    """

    env = jinja2.Environment(loader=jinja2.PackageLoader('serp_crawler', 'templates'))
    tpl_rendered = env.get_template('page_crawler.k8s.yaml.jinja2').render(
        job_name=f'scriptor-serp-crawl-{uuid.uuid4()}',
        config_path=config_path,
        input_path=input_path,
        output_path=output_path
    )
    with subprocess.Popen(['kubectl', 'apply', '-f', '-'], stdin=subprocess.PIPE) as proc:
        proc.communicate(input=tpl_rendered.encode())


@main.command()
@click.argument('result-file', type=click.File('r'), nargs=-1, required=True)
@click.option('-n', '--cutoff', type=int, help='Rank cutoff')
@click.option('-s', '--top-suffixes', type=int, help='List n top public suffixes', default=10)
@click.option('-q', '--bottom-queries', type=int, help='List n queries with fewest hits', default=10)
@click.option('-p', '--plot-counts', is_flag=True, help='Show plot of suffix counts')
def analyze(result_file, cutoff, top_suffixes, bottom_queries, plot_counts):
    """Analyze crawl result file."""

    import publicsuffix
    if plot_counts:
        from matplotlib import pyplot as plt

    ps = publicsuffix.PublicSuffixList()

    queries_processed = set()
    unique_urls = set()
    unique_hosts = set()
    youtube_urls = []
    suffixes = []
    num_hits = {}
    num_ads = 0
    for rf in result_file:
        for line in rf:
            result = json.loads(line)
            if result['query'] in queries_processed:
                continue
            queries_processed.add(result['query'])
            ads_skipped = 0
            cutoff_effective = cutoff or len(result['results'])
            for i, hit in enumerate(result['results']):
                if i >= cutoff_effective:
                    break
                if hit['url'].startswith('https://duckduckgo.com/y.js'):
                    # Ignore ads
                    ads_skipped += 1
                    cutoff_effective += 1
                    continue
                unique_urls.add(hit['url'])
                parsed_url = urlparse.urlparse(hit['url'])
                unique_hosts.add(parsed_url.netloc)
                s = ps.get_public_suffix(parsed_url.netloc)
                if s == 'youtube.com':
                    youtube_urls.append(hit['url'])
                suffixes.append(s)

            num_hits[(rf.name, result['query'])] = min(len(result['results']), cutoff_effective) - ads_skipped
            num_ads += ads_skipped

    suffix_counts = sorted(Counter(suffixes).items(), key=lambda x: -x[1])

    print(f'Total Hits: {sum(num_hits.values()):,d}')
    print(f'Unique URLs: {len(unique_urls):,d}')
    print(f'Unique hosts: {len(unique_hosts):,d}')
    print(f'Unique public suffixes: {len(set(suffixes)):,d}')
    print(f'Public suffix singletons: {sum([c[1] for c in suffix_counts if c[1] == 1]):,d}')
    print(f'Average number of hits: {statistics.mean(num_hits.values()):.2f}')
    print(f'Median number of hits: {statistics.median(num_hits.values())}')
    print(f'Max number of hits: {max(num_hits.values())}')
    print(f'Min number of hits: {min(num_hits.values())}')
    print(f'Ads skipped: {num_ads}')
    print(f'YouTube URLs: {len(youtube_urls):,d}')
    print(f'Unique YouTube URLs: {len(set(youtube_urls)):,d}')
    print()

    print(f'Top {top_suffixes} public suffixes:')
    for s, c in suffix_counts[:top_suffixes]:
        print(f' - {s} ({c})')
    print()

    print(f'Bottom {bottom_queries} queries with fewest hits:')
    for q, c in sorted(num_hits.items(), key=lambda x: x[1])[:bottom_queries]:
        print(f' - {q[1]} ({c})')

    if plot_counts:
        plt.hist([c[1] for c in suffix_counts], bins=50)
        plt.xlabel('Suffix Frequency')
        # plt.xscale('log')
        plt.ylabel('Suffix Count')
        plt.yscale('log')
        plt.show()


@main.command()
@click.argument('result-file', type=click.File('r'))
@click.argument('output', type=click.File('w'))
def youtube_transcripts(result_file, output):
    """Download transcripts for crawled YouTube videos."""

    for line in tqdm(result_file, desc='Extracting YouTube transcripts from SERPs', unit=' SERPs'):
        serp = json.loads(line)
        for i, hit in enumerate(serp['results']):
            url = urlparse.urlparse(hit['url'])
            if url.hostname not in ['www.youtube.com', 'youtube.com']:
                continue
            video_id = urlparse.parse_qs(url.query).get('v')
            if not video_id:
                continue

            wait = 0
            backoff = None
            while True:
                try:
                    transcript = YouTubeTranscriptApi.get_transcript(video_id[0], languages=['en'])
                    output.write(json.dumps(dict(
                        query=serp['query'],
                        hit=i,
                        url=hit['url'],
                        transcript=transcript
                    )) + '\n')
                    break
                except TooManyRequests:
                    wait += 1
                    if not backoff:
                        backoff = tqdm(desc='Too many requests, backing off...', leave=False)
                    backoff.update()
                    time.sleep(2 ** wait)
                except CouldNotRetrieveTranscript:
                    break


@main.command()
@click.argument('transcript-file', type=click.File('r'))
@click.argument('output-dir', type=click.Path(file_okay=False))
def youtube_extract_transcripts(transcript_file, output_dir):
    """Extract downloaded YouTube transcripts to text files."""

    os.makedirs(output_dir, exist_ok=True)
    for i, line in enumerate(tqdm(transcript_file, desc='Extracting YouTube transcripts', unit=' transcripts')):
        transcript = json.loads(line)

        durations = [t['duration'] for t in transcript['transcript']]

        with open(os.path.join(output_dir, f'transcript_{i:04d}.txt'), 'w') as out:
            avg_duration = sum(durations) / len(durations)
            out.write(f'Avg. duration: {avg_duration:.2f}s\n\n')
            out.write('\n'.join(t['text'] for t in transcript['transcript']))


def validate_url(url):
    request_headers = {
        'User-Agent': choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.8',
        'Accept-Encoding': 'gzip,deflate',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    parked_patterns = [
        r'(domain|{domain}) (is|may be) (for sale|available)',
        r'(domain|{domain}) ist verfügbar',
        r'this (page|domain) (is|has been) (suspended|parked)',
        r'buy (this )?(domain|{domain})',
        r'({domain}|this domain) was (successfully )?registered',
        r'({domain}|this domain) (was|has been) registered with',
        r'parked page',
        r'domain parking',
        r'related links',
        r'sell your domain',
        r'domains? kaufen',
        r'interested in ({domain}|this domain)\?',
        r'search for domains similar to',
        r'the best domains',
    ]

    logger.info('Validating %s', url)

    url_parsed = urlparse.urlparse(url)

    try:
        socket.getaddrinfo(url_parsed.netloc, 443)
    except socket.gaierror:
        return 'nxdomain', 1

    try:
        resp = httpx.get(url, headers=request_headers, follow_redirects=True)
    except httpx.TransportError:
        return 'no_response', 1
    except httpx.TooManyRedirects:
        return 'too_many_redirects', 1
    except:
        return 'unknown_exception', 1

    if resp.status_code != 200:
        if resp.status_code in [401, 403, 404, 500, 502]:
            return f'http_{resp.status_code}', 1
        return 'http_other', 1

    tree = HTMLTree.parse_from_bytes(resp.read(), resp.encoding)
    plaintext = extract_plain_text(tree, preserve_formatting=False, main_content=False, list_bullets=False)
    if len(plaintext) < 500:
        for p in parked_patterns:
            if re.search(p.format(domain=url_parsed.netloc.replace('.', r'\.')), plaintext.lower()):
                return 'likely_parked', 1

    return 'ok', 1


@main.command()
@click.argument('result-file', type=click.Path(dir_okay=False, exists=True))
@click.argument('output', type=click.Path(dir_okay=False))
@click.option('-p', '--parallelism', type=int, default=150, help='Number of workers')
def validate_serps(result_file, output, parallelism):
    """
    Check SERPs for whether they (still) point to valid websites and aggregate error statistics.
    """

    options = get_pipeline_opts(parallelism=parallelism)
    with beam.Pipeline(options=options) as pipeline:
        _ = (
            pipeline
            | 'Load SERPs' >> ReadFromText(result_file, desired_split_size=256 * 1024, min_split_size=64 * 1024)
            | 'Extract URLs' >> beam.FlatMap(lambda l: [r['url'] for r in json.loads(l)['results']])
            | 'Validate URLs' >> beam.Map(validate_url)
            | 'Combine Stats' >> beam.CombinePerKey(sum)
            | 'To Dict' >> beam.transforms.combiners.ToDict()
            | 'Serialize' >> beam.Map(json.dumps)
            | 'Write Output' >> beam.io.WriteToText(output)
        )


if __name__ == '__main__':
    main()
