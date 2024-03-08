# Copyright 2021 Janek Bevendorff, Webis
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

from base64 import b64encode
from collections import Counter
from email.utils import parsedate_to_datetime
from glob import glob
from hashlib import sha1
import json
import logging
import numpy as np
import os
import re
from statistics import mean, median, stdev
import sys
import tarfile
from urllib import parse as urlparse
import requests

import apache_beam as beam
import apache_beam.typehints as t
from apache_beam.io import fileio,  filesystems
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions
import click
from elasticsearch import Elasticsearch, helpers as es_helpers
from fastwarc.warc import ArchiveIterator, WarcRecord, WarcRecordType
from fastwarc.stream_io import StreamError
import publicsuffix
from resiliparse.beam.elasticsearch import ElasticsearchBulkIndex, index_action
from resiliparse.beam.fileio import MatchFiles
from resiliparse.beam.warcio import ReadAllWarcs
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.lang import detect_fast, train_language_examples
from resiliparse.parse.encoding import detect_encoding
from textstat import textstat
from tqdm import tqdm

from warc_analysis.config import get_config
from warc_analysis.es_scroll import ElasticsearchScroll


logger = logging.getLogger()
PATH = os.path.dirname(__file__)


@click.group()
def main():
    pass


AFFILIATE_URL_PATTERNS = {
    'amazon': re.compile(r'^https?://(?:www\.)?amazon\.[a-z]{2,3}/.+?\?(?:[^&]*?&)*?tag='),
    'ebay': re.compile(r'^https?://(?:www\.)ebay\.[a-z]{2,3}/.+?\?(?:[^&]*?&)*?mkrid='),
    'ali': re.compile(r'^https?://s\.click\.aliexpress\.[a-z]{2,3}/e/'),
    'shareasale': re.compile(r'^https?://(?:www\.)?(?:shareasale\.com/r.cfm\?|shrsl\.com)'),
    'awin': re.compile(r'^https?://(?:www\.)?awin1\.com'),
    'clickbank': re.compile(r'^https?://.+?\.hop\.clickbank\.net'),
    'cj': re.compile(r'^https?://(?:www\.)?(?:jdoqocy\.com/click-|anrdoezrs\.net/links/)'),
    'flexoffer': re.compile(r'^https?://track\.flexlinks(?:pro)?.com/a\.ashx\?'),
    'refersion': re.compile(r'^https?://.+?\?rfsn='),
    'pepperjam': re.compile(r'^https?://pjtra\.com/t/') ,
    'rakuten': re.compile(r'^https?://linksynergy\..+?\.com/t/'),
    'webgain': re.compile(r'^https?://track\.webgains\.com/click\.html\?'),
}

AMZN_PRODUCT_PATTERN = re.compile(r'^/?[^/]*/[gd]p/')
URL_SHORTENER_PATTERN = re.compile(r'^https?://(?:amzn\.to|ebay\.us|fxo\.co|bit\.ly)/')
WORD_REGEX = re.compile(r'[\w-]+', re.UNICODE)

# Affiliate URL examples:
#
# Amazon:       https://www.amazon.com/dp/XXXX/YYY?tag=ZZZZ
# Ebay:         https://www.ebay.com/itm/274751732578?_trkparms=XXX&_trkparms=XXX&mkcid=1&mkrid=XXX&siteid=0&campid=5338615654&customid=&toolid=10001&mkevt=1
# Shareasale:   http://www.shareasale.com/r.cfm?B=XXX&M=47864&urllink=
#               http://shrsl.com/?~7na7
# Awin:         http://www.awin1.com/
# Clickbank:    https://<some-id>.hop.clickbank.net
# CJ:           http://www.jdoqocy.com/click-xxxxxxx-xxxxxxx
#               https://www.anrdoezrs.net/links/8203012/type/dlg/<url>
# Flexoffer:    http://track.flexlinks.com/a.ashx?foid=177.A171465&foc=1&fot=9999&fos=1&url=<url>
#               https://track.flexlinkspro.com/a.ashx?...
#               (a.ashx for links and i.ashx for image sources,
#               see https://support.flexoffers.com/hc/en-us/articles/360042910571-URL-Shortener-Short-Links)
# Refersion:    https://www.yourdomain.com?rfsn=123456.abc123
# Pepperjam:    https://pjtra.com/t/<hash>?url=<url>
# Rakuten:      https://linksynergy.<some-domain>.com/t/<hash>?url=<url>
# Webgain:      https://track.webgains.com/click.html?wgcampaignid...


ENGLISH_FUNC_WORDS = {'a', 'about', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost',
                      'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'amoungst',
                      'an', 'and', 'another', 'any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere', 'are',
                      'around', 'as', 'at', 'be', 'became', 'because', 'been', 'before', 'beforehand', 'behind',
                      'being', 'below', 'beside', 'besides', 'between', 'beyond', 'both', 'but', 'by', 'can', 'cannot',
                      'could', 'dare', 'despite', 'did', 'do', 'does', 'done', 'down', 'during', 'each', 'eg', 'either',
                      'else', 'elsewhere', 'enough', 'etc', 'even', 'ever', 'every', 'everyone', 'everything',
                      'everywhere', 'except', 'few', 'first', 'for', 'former', 'formerly', 'from', 'further',
                      'furthermore', 'had', 'has', 'have', 'he', 'hence', 'her', 'here', 'hereabouts', 'hereafter',
                      'hereby', 'herein', 'hereinafter', 'heretofore', 'hereunder', 'hereupon', 'herewith', 'hers',
                      'herself', 'him', 'himself', 'his', 'how', 'however', 'i', 'ie', 'if', 'in', 'indeed', 'inside',
                      'instead', 'into', 'is', 'it', 'its', 'itself', 'last', 'latter', 'latterly', 'least', 'less',
                      'lot', 'lots', 'many', 'may', 'me', 'meanwhile', 'might', 'mine', 'more', 'moreover', 'most',
                      'mostly', 'much', 'must', 'my', 'myself', 'namely', 'near', 'need', 'neither', 'never',
                      'nevertheless', 'next', 'no', 'nobody', 'none', 'noone', 'nor', 'not', 'nothing', 'now',
                      'nowhere', 'of', 'off', 'often', 'oftentimes', 'on', 'once', 'one', 'only', 'onto', 'or', 'other',
                      'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'per',
                      'perhaps', 'rather', 're', 'same', 'second', 'several', 'shall', 'she', 'should', 'since', 'so',
                      'some', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere',
                      'still', 'such', 'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence',
                      'there', 'thereabouts', 'thereafter', 'thereby', 'therefore', 'therein', 'thereof', 'thereon',
                      'thereupon', 'these', 'they', 'third', 'this', 'those', 'though', 'through', 'throughout', 'thru',
                      'thus', 'to', 'together', 'too', 'top', 'toward', 'towards', 'under', 'until', 'up', 'upon', 'us',
                      'used', 'very', 'via', 'was', 'we', 'well', 'were', 'what', 'whatever', 'when', 'whence',
                      'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever',
                      'whether', 'which', 'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why',
                      'whyever', 'will', 'with', 'within', 'without', 'would', 'yes', 'yet', 'you', 'your', 'yours',
                      'yourself', 'yourselves'}

REVIEW_KEYWORD_PATTERN = re.compile(r'^(?:The )?(?:\d{1,2} |Five |Ten )?(Best|Top) .{5,}|(Best|Top) .+ 20[12]\d|'
                                    r' we(?:\'ve| have) tested\b|\bto buy\b|\btop picks\b|.+ Review\b|. compared$|'
                                    r'What is the best .+|Our favou?rite\b|How to shop for\b|\bthat actually works?\b|'
                                    r'starter kit\b|\d{1,2} tips to |How to use ', re.I)


def is_nofollow_link(element):
    for a in element.getattr('rel', '').split(' '):
        if a in ('nofollow', 'sponsored', 'ugc'):
            return True
    return False


def keyword_overlap(keywords, body_text_tokens):
    keywords = {k.lower() for k in keywords if k not in ENGLISH_FUNC_WORDS}
    return safe_div(len([t for t in body_text_tokens if t.lower() in keywords]), len(body_text_tokens))


def calc_word_metrics(text):
    """Calculate word-level metrics."""
    types = set()
    token_count = 0
    token_re = re.compile(r'\w+', re.U)
    func_count = 0
    for token in token_re.finditer(text):
        types.add(token.group(0))
        token_count += 1
        func_count += int(token.group(0) in ENGLISH_FUNC_WORDS)

    return dict(
        num_chars=len(text),
        num_words=token_count,
        ttr=safe_div(len(types), token_count),
        fwr=safe_div(func_count, token_count)
    )


def safe_div(a, b):
    """Safely divide two numbers, return zero if b is zero."""
    return a / b if b != 0 else 0.0


def http_date_to_iso(http_date):
    """Safely try to parse an HTTP date to an ISO datetime or return None if date string is invalid."""
    if http_date is None:
        return None

    try:
        return parsedate_to_datetime(http_date).isoformat().replace('+00:00', 'Z')
    except (ValueError, TypeError):
        return None


class PublicSuffixList:
    """
    Wrapper around :class:`publicsuffix.PublicSuffixList` that automatically
    downloads the current list at construction.
    """

    def __init__(self):
        with publicsuffix.fetch() as f:
            self.psl = publicsuffix.PublicSuffixList(f)

    def get_public_suffix(self, domain):
        suffix = domain.encode().decode('idna')
        if suffix.endswith('.wordpress.com'):
            # *.wordpress.com is not in the public suffix list
            return suffix[suffix.rfind('.', 0, -14) + 1:]
        return self.psl.get_public_suffix(suffix)


# noinspection PyBroadException,PyAbstractClass
class ProcessRecord(beam.DoFn):
    """Process a WARC record."""

    def __init__(self, search_engine_name=None, is_clueweb22=False, crawl_date=None):
        super().__init__()
        self.psl = None    # type: PublicSuffixList | None
        self._search_engine_name = search_engine_name
        self._is_clueweb22 = is_clueweb22
        self._crawl_date = crawl_date

    def setup(self):
        self.psl = PublicSuffixList()

    def teardown(self):
        self.psl = None

    # noinspection PyMethodOverriding
    def process(self, element: t.Tuple[str, WarcRecord]):
        file_name, warc_record = element    # type: (str, WarcRecord)
        doc_id = warc_record.record_id
        query = None
        if type(file_name) is tuple:
            file_name, query, doc_id = file_name

        try:
            content_bytes = warc_record.reader.read()
        except StreamError as e:
            logger.error('Error reading stream of record %s:', f'{file_name}:{warc_record.record_id}')
            logger.exception(e)
            return

        if warc_record.content_length < 1500:
            logger.info('Skipped %s (content too short)', warc_record.record_id)
            return

        if not self._is_clueweb22:
            if not warc_record.http_content_type or \
                    warc_record.http_content_type.lower() not in ['text/html', 'application/xhtml+xml', 'text/plain']:
                logger.info('Skipped %s (wrong Content-Type %s)',
                            warc_record.record_id, warc_record.http_content_type)
                return

            encoding = warc_record.http_charset or detect_encoding(content_bytes)
        else:
            encoding = 'utf-8'

        tree = HTMLTree.parse_from_bytes(content_bytes, encoding)
        if not tree.body:
            logger.info('Skipped %s (failed to parse HTML)', warc_record.record_id)
            return

        main_content = extract_plain_text(tree,
                                          preserve_formatting=False,
                                          main_content=True,
                                          list_bullets=False,
                                          alt_texts=False,
                                          links=False,
                                          form_fields=False,
                                          comments=False)

        main_content_tokenized = WORD_REGEX.findall(main_content)

        warc_target_uri = warc_record.headers['WARC-Target-URI']
        warc_target_uri_parsed = urlparse.urlparse(warc_target_uri)
        # Decode ChatNoir web cache URLs if necessary
        if warc_target_uri_parsed.netloc == 'chatnoir-webcontent.web.webis.de':
            query_parsed = urlparse.parse_qs(warc_target_uri_parsed.query)
            if 'url' not in query_parsed:
                logger.error('Invalid ChatNoir URL: %s', warc_target_uri)
                return
            warc_target_uri = query_parsed['url'][0]
            warc_target_uri_parsed = urlparse.urlparse(warc_target_uri)
        main_content_stats = calc_word_metrics(main_content)

        lang = detect_fast(main_content)
        if lang[0] != 'en':
            return

        # Count links and affiliate links
        link_stats = {
            'links': 0,
            'self_links': 0,
            'nofollow_links': 0,
            'anchor_chars_mean': 0,
            'anchor_words_mean': 0,
            'affiliate_links': 0,
            'affiliate_anchor_chars_mean': 0,
            'affiliate_anchor_words_mean': 0,
            'affiliate_product_links': 0,
            'affiliate_nofollow_links': 0,
            'affiliate_links_shortened': 0,
        }
        link_network_stats = {k: 0 for k in AFFILIATE_URL_PATTERNS.keys()}

        for e in tree.body.get_elements_by_tag_name('a'):
            link_stats['links'] += 1
            link_stats['anchor_chars_mean'] += len(e.text)
            link_stats['anchor_words_mean'] += len(WORD_REGEX.findall(e.text))
            link_stats['nofollow_links'] += int(is_nofollow_link(e))

            href_url = e.getattr('href', '').strip()
            if not href_url:
                continue

            try:
                url_parsed = urlparse.urlparse(href_url)
            except Exception:
                continue

            if url_parsed.netloc == warc_target_uri_parsed.netloc or (
                    url_parsed.scheme == url_parsed.netloc == '' and url_parsed.path != ''):
                link_stats['self_links'] += 1
                continue

            shortlink_resolved = False
            if URL_SHORTENER_PATTERN.match(href_url):
                try:
                    # Send HEAD request and extract the response Location header without following the redirect
                    resp = requests.head(href_url, allow_redirects=False)
                    if not (300 <= resp.status_code < 400) or 'Location' not in resp.headers:
                        raise ValueError(f'Wrong status code ({resp.status_code}) or no Location header set')
                    href_url = resp.headers['Location']
                    url_parsed = urlparse.urlparse(href_url)
                    shortlink_resolved = True
                except Exception as e:
                    logger.error('Failed to resolve shortened URL %s: %s', href_url, e)
                    continue

            for nw, rx in AFFILIATE_URL_PATTERNS.items():
                if not rx.match(href_url):
                    continue

                link_stats['affiliate_links'] += 1
                link_network_stats[nw] += 1
                link_stats['affiliate_links_shortened'] += int(shortlink_resolved)
                link_stats['affiliate_nofollow_links'] += int(is_nofollow_link(e))
                anchor_text = e.text
                link_stats['affiliate_anchor_chars_mean'] += len(anchor_text)
                link_stats['affiliate_anchor_words_mean'] += len(WORD_REGEX.findall(anchor_text))
                link_stats['affiliate_product_links'] += int(nw == 'amazon' and
                                                             AMZN_PRODUCT_PATTERN.match(url_parsed.path) is not None)
                break

        link_stats['anchor_chars_mean'] = safe_div(link_stats['anchor_chars_mean'], link_stats['links'])
        link_stats['anchor_words_mean'] = safe_div(link_stats['anchor_words_mean'], link_stats['links'])

        link_stats['affiliate_anchor_chars_mean'] = safe_div(link_stats['affiliate_anchor_chars_mean'],
                                                             link_stats['affiliate_links'])
        link_stats['affiliate_anchor_words_mean'] = safe_div(link_stats['affiliate_anchor_words_mean'],
                                                             link_stats['affiliate_links'])

        # Meta description stats
        meta_desc = ''
        meta_keywords = []
        if tree.head:
            meta_desc_el = tree.head.query_selector('meta[name="description"]')
            if meta_desc_el:
                meta_desc = meta_desc_el.getattr('content', '')
            meta_keywords_el = tree.head.query_selector('meta[name="keywords"]')
            if meta_keywords_el:
                meta_keywords = WORD_REGEX.findall(meta_keywords_el.getattr('content', ''))
        meta_desc_stats = {'meta_' + k: v for k, v in calc_word_metrics(meta_desc).items()}

        # H1 heading stats
        h1_elements = tree.body.get_elements_by_tag_name('h1')
        h1_stats = {'h1_' + k: v for k, v in calc_word_metrics(h1_elements[0].text if h1_elements else '').items()}

        # Check headings against review keyword regex
        heading_elements = tree.body.query_selector_all('h1, h2, h3, h4, h5, h6')
        is_review = False
        heading_texts = []
        for h in heading_elements:
            heading_texts.append(h.text)
            if REVIEW_KEYWORD_PATTERN.search(heading_texts[-1]):
                is_review = True
                break

        # Calculate heading / meta to body keyword overlap
        heading_keyword_overlap = keyword_overlap(WORD_REGEX.findall(' '.join(heading_texts)), main_content_tokenized)
        meta_keyword_overlap = keyword_overlap(WORD_REGEX.findall(meta_desc) + meta_keywords, main_content_tokenized)

        # P and Hx to main content word ratio
        num_ph = len(tree.body.get_elements_by_tag_name('p')) + len(heading_elements)
        ph_ratio = np.log(1 + safe_div(len(main_content_tokenized), num_ph))

        # Images
        img_elements = tree.body.get_elements_by_tag_name('img')
        num_img = len(img_elements)
        img_stats = Counter(calc_word_metrics(''))
        for e in img_elements:
            if len(e.getattr('alt', '')) == 0:
                continue
            img_stats += Counter(calc_word_metrics(e['alt']))
        img_stats = {'img_' + k: safe_div(v, num_img) for k, v in img_stats.items()}

        # Anchors
        a_elements = tree.body.get_elements_by_tag_name('a')
        num_a = len(a_elements)
        a_stats = Counter(calc_word_metrics(''))
        for e in a_elements:
            a_stats += Counter(calc_word_metrics(e.text))
        a_stats = {'a_' + k: safe_div(v, num_a) for k, v in a_stats.items()}

        warc_target_uri_path = warc_target_uri_parsed.path
        if warc_record.is_http_parsed:
            http_date = http_date_to_iso(warc_record.http_headers.get('Date'))
            http_last_modified = http_date_to_iso(warc_record.http_headers.get('Last-Modified'))
        else:
            http_date = None
            http_last_modified = None

        yield dict(
            _id=doc_id,
            src_file=file_name,
            src_offset=warc_record.stream_pos,
            search_engine=self._search_engine_name,
            query=query,
            record_id=warc_record.record_id,
            scheme=warc_target_uri_parsed.scheme,
            host=warc_target_uri_parsed.netloc,
            public_suffix=self.psl.get_public_suffix(warc_target_uri_parsed.netloc),
            warc_date=warc_record.record_date.isoformat().replace('+00:00', 'Z'),
            crawl_date=self._crawl_date.isoformat().split('T')[0] if self._crawl_date else None,
            date=http_date,
            last_modified=http_last_modified,
            url=warc_target_uri,
            url_path_chars=len(warc_target_uri_path),
            url_path_depth=warc_target_uri_path.count('/'),
            url_hyphen_path_ratio=safe_div(warc_target_uri_path.count('-'), len(warc_target_uri_path)),
            url_path_digits=sum(c.isdigit() for c in warc_target_uri_path),
            **link_stats,
            **{'affiliate_links_' + k: v for k, v in link_network_stats.items()},
            anchor_chars_ratio=safe_div(link_stats['anchor_chars_mean'], main_content_stats['num_chars']),
            anchor_words_ratio=safe_div(link_stats['anchor_words_mean'], main_content_stats['num_words']),
            self_link_ratio=safe_div(link_stats['self_links'], link_stats['links']),
            title_chars=len(tree.title),
            is_review=is_review,
            num_h1=len(h1_elements),
            num_h2=len(tree.body.get_elements_by_tag_name('h2')),
            num_img=num_img,
            **main_content_stats,
            **meta_desc_stats,
            **h1_stats,
            **img_stats,
            **a_stats,
            num_meta_keywords=len(meta_keywords),
            ph_ratio=ph_ratio,
            heading_keyword_overlap=heading_keyword_overlap,
            meta_keyword_overlap=meta_keyword_overlap,
            num_ldjson=len(tree.body.query_selector_all('script[type="application/ld+json"]')),
            has_og=tree.head is not None and tree.head.query_selector('meta[property^="og:"]') is not None,
            has_bread=tree.body.query_selector(
                '[itemtype$="//schema.org/BreadcrumbList"], [class*="breadcrumb"]') is not None,
            flesch_ease=float(min(100, max(0, textstat.flesch_reading_ease(main_content)))),
            flesch_grade=float(textstat.flesch_kincaid_grade(main_content)),
            main_content=main_content,
            word_freq_vec=b64encode(np.asarray(
                train_language_examples([main_content]), dtype=np.uint8).tobytes()).decode()
        )


# noinspection PyAbstractClass
class MapToPublicSuffixKey(beam.DoFn):
    """Map elements with domain name keys to their shortened public suffixes."""

    def __init__(self):
        super().__init__()
        self.psl = None    # type: PublicSuffixList | None

    def setup(self):
        self.psl = PublicSuffixList()

    def teardown(self):
        self.psl = None

    # noinspection PyMethodOverriding
    def process(self, element: t.KV[str, t.Any]) -> t.Iterable[t.KV[str, t.Any]]:
        yield self.psl.get_public_suffix(element[0]), element[1]


# noinspection PyAbstractClass
class ReadWarcsFromTar(beam.DoFn):
    """
    Read records from WARC files stored in a TAR file.

    :param first_html_response: return only first non-redirect HTML response record from each WARC
    :param warc_args: additional arguments to pass to FastWARCs ArchiveIterator
    """

    def __init__(self, first_html_response=True, warc_args=None):
        super().__init__()
        self.first_html_response = first_html_response
        self.warc_args = warc_args or {}

    # noinspection PyMethodOverriding
    def process(self, file_meta):
        with filesystems.FileSystems.open(file_meta.path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            tarf = tarfile.TarFile(fileobj=f)

            query_id = os.path.basename(file_meta.path)[:-4]
            query = tarf.extractfile(f'{query_id}/query.txt').read().strip()
            query_digest = sha1(query).hexdigest()

            for member in tarf:
                if not member.name.endswith('.warc.gz'):
                    continue

                file_virt_name = f'{file_meta.path}:{member.name}'
                hit_id = member.name.split('/')[1][4:]

                if member.size == 0:
                    logger.error(f'Empty WARC file %s.', file_virt_name)
                    continue

                try:
                    for rec in ArchiveIterator(tarf.extractfile(member),
                                               record_types=WarcRecordType.response, parse_http=True,
                                               auto_decode='content', **self.warc_args):

                        if self.first_html_response:
                            if rec.http_content_type not in ('text/html', 'application/xhtml+xml') or \
                                    300 <= rec.http_headers.status_code < 400:
                                continue

                        rec.freeze()
                        yield (file_virt_name, query.decode(), ':'.join([query_digest, hit_id])), rec

                        if self.first_html_response:
                            break

                except StreamError as e:
                    logger.error(f'Error reading WARC file %s:', file_virt_name)
                    logger.exception(e)


def get_pipeline_opts(**override_args):
    """Construct PipelineOpts object from config with override arguments."""
    cfg = get_config()['pipeline_opts'].copy()
    cfg.update(override_args)
    return PipelineOptions(**cfg)


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('input-glob')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('-o', '--output-dir', type=str, help='Write extracted data to JSONL')
@click.option('-s', '--max-content-length', type=int, default=1024 * 1024, show_default=True,
              help='Maximum record Content-Length in bytes')
@click.option('-i', '--meta-index', help='Elasticsearch metadata index',
              default='affiliate_cc_stats', show_default=True)
@click.option('-p', '--parallelism', type=int, help='Processing parallelism')
@click.option('-t', '--parse-tars', is_flag=True, help='Treat input glob as set of TAR files containing warc.gz files')
@click.option('-c', '--clueweb22', is_flag=True, help='Enable lenient parsing for ClueWeb22 WARCs')
@click.option('-n', '--search-engine', help='Name of source search engine to index')
@click.option('-d', '--crawl-date', type=click.DateTime(formats=['%Y-%m-%d']), help='Crawl date to index')
def process(input_glob, beam_args, output_dir, max_content_length, meta_index, parallelism, parse_tars,
            clueweb22, search_engine, crawl_date):
    """
    Process WARCs and extract affiliate data.
    """

    sys.argv[1:] = beam_args
    options = get_pipeline_opts(parallelism=parallelism)

    warc_args = dict(
        record_types=int(WarcRecordType.response),
        max_content_length=max_content_length
    )

    if clueweb22:
        warc_args.update(dict(
            strict_mode=False,
            parse_http=False))

    with beam.Pipeline(options=options) as pipeline:
        in_files = pipeline | MatchFiles(input_glob)

        if parse_tars:
            in_files |= 'Read WARCs from TAR' >> beam.ParDo(ReadWarcsFromTar(first_html_response=True))
        else:
            in_files |= 'Iterate WARCs' >> ReadAllWarcs(warc_args=warc_args,
                                                        freeze=False,
                                                        with_filename=True)

        records = in_files | 'Process Records' >> beam.ParDo(ProcessRecord(search_engine, clueweb22, crawl_date))
        if output_dir:
            _ = (
                records
                | 'Serialize JSON' >> beam.Map(json.dumps)
                | 'Write JSONL' >> fileio.WriteToFiles(output_dir,
                                                       file_naming=fileio.default_file_naming('j_output', '.jsonl'))
            )

        __ = (
            records
            | 'Strip Main Content' >> beam.Map(lambda e: {k: v for k, v in e.items() if k != 'main_content'})
            | 'Map Index Actions' >> beam.Map(lambda e: index_action(e['_id'], meta_index, e))
            | 'Index Metadata' >> ElasticsearchBulkIndex(get_config()['elasticsearch'],
                                                         buffer_size=400, chunk_size=400)
        )


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('input-glob')
@click.argument('output-index')
def index_gpt_scores(input_glob, output_index):
    """
    Read GPT scores from JSONL file and update Elasticsearch index documents.
    """

    import orjson

    client = Elasticsearch(**get_config()['elasticsearch'])
    update_bulk = []
    for filename in tqdm(glob(input_glob), desc='Reading input files', unit=' file', leave=False):
        for line in tqdm(open(filename, 'rb'), desc='Updating index docs', unit=' docs', leave=False):
            rec = orjson.loads(line)
            update_bulk.append({
                '_op_type': 'update',
                '_index': output_index,
                '_id': rec['record_id'],
                'doc': {'gpt_page_quality': rec['gpt_page_quality']}
            })
            if len(update_bulk) > 2000:
                es_helpers.bulk(client, update_bulk, raise_on_error=False)
                update_bulk.clear()


def agg_doc_values(docs):
    """Aggregate list of documents by averaging numeric values and discarding non-numeric values."""
    if not docs:
        return {}
    if type(docs) not in (list, tuple):
        docs = list(docs)

    result_doc = {}
    keys = set().union(*docs)
    for k in keys:
        # Add ints/floats
        vals = [docs[i][k] for i in range(len(docs)) if k in docs[i] and type(docs[i][k]) in (int, float)]
        if not vals:
            # Check if bool if no numbers found
            vals = [docs[i][k] for i in range(len(docs)) if k in docs[i] and type(docs[i][k]) is bool]
            if not vals:
                continue
            # Use majority vote for bools
            result_doc['maj_' + k] = (sum(vals) + 1) // len(docs)
            continue

        # Average numbers
        result_doc['avg_' + k] = mean(vals)
        result_doc['median_' + k] = median(vals)
        result_doc['stddev_' + k] = stdev(vals) if len(vals) > 1 else 0

    result_doc['num_docs'] = len(docs)

    return result_doc


@main.command(context_settings=dict(
    ignore_unknown_options=True
))
@click.argument('input-index')
@click.argument('output-index')
@click.argument('beam-args', nargs=-1, type=click.UNPROCESSED)
@click.option('-p', '--parallelism', type=int, default=10, help='Processing parallelism')
@click.option('-s', '--scroll-parallelism', type=int, default=10, help='Elasticsearch scroll parallelism')
def aggregate_metadata(input_index, output_index, beam_args, parallelism, scroll_parallelism):
    """
    Aggregate index meta data from one index into another.
    """

    sys.argv[1:] = beam_args
    options = get_pipeline_opts(parallelism=parallelism)

    with beam.Pipeline(options=options) as pipeline:
        _ = (
            pipeline
            | 'Read Elasticsearch' >> ElasticsearchScroll(get_config()['elasticsearch'],
                                                          index=input_index, parallelism=scroll_parallelism)
            | 'Map Domain Names' >> beam.Map(lambda e: (e['_source']['host'], e['_source']))
            | 'Map Public Suffix' >> beam.ParDo(MapToPublicSuffixKey())
            | 'Group By Suffix' >> beam.GroupByKey()
            | 'Aggregate Values' >> beam.MapTuple(lambda k, v: (k, agg_doc_values(v)))
            | 'Index Aggregates' >> ElasticsearchBulkIndex(get_config()['elasticsearch'],
                                                           default_index=output_index, buffer_size=400, chunk_size=400)
        )
