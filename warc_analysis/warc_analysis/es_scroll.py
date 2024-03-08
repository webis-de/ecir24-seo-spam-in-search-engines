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

# Elasticsearch scroll PTransform to be merged into Resiliparse

from functools import partial
import logging
import time

import apache_beam as beam
import apache_beam.typehints as t
from elasticsearch import exceptions as es_exc, Elasticsearch

logger = logging.getLogger()


def _retry_with_backoff(fn, max_retries, initial_backoff, max_backoff):
    retry_count = 0
    while retry_count <= max_retries:
        try:
            return fn()
        except es_exc.TransportError as e:
            logger.error('Elasticsearch transport error (attempt %i/%i): %s',
                         retry_count + 1, max_retries + 1, e.message)
            if retry_count >= max_retries:
                raise e

        logger.error('Retrying with exponential backoff in %i seconds...',
                     initial_backoff * (2 ** retry_count))
        time.sleep(min(max_backoff, initial_backoff * (2 ** retry_count)))
        retry_count += 1


class ElasticsearchScroll(beam.PTransform):
    """Scroll through results of an Elasticsearch query in parallel."""

    def __init__(self,
                 es_args: t.Dict[str, t.Any],
                 index: str,
                 search_body: t.Dict[str, t.Any] = None,
                 search_size: int = 800,
                 parallelism: int = None,
                 reshuffle: bool = True,
                 return_kv: bool = False,
                 scroll_duration: str = '10m',
                 clear_scroll: bool = True,
                 max_retries: int = 10,
                 initial_backoff: float = 2,
                 max_backoff: float = 600,
                 request_timeout: int = 240,
                 **search_args):

        super().__init__()
        self._dofn = _ElasticsearchScrollFn(es_args=es_args,
                                            index=index,
                                            search_body=search_body,
                                            search_size=search_size,
                                            search_args=search_args,
                                            scroll_duration=scroll_duration,
                                            clear_scroll=clear_scroll,
                                            max_retries=max_retries,
                                            initial_backoff=initial_backoff,
                                            max_backoff=max_backoff,
                                            request_timeout=request_timeout,
                                            return_kv=return_kv)
        self._parallelism = parallelism or 1
        self._reshuffle = reshuffle

    def expand(self, pcoll):
        slices = [(i, self._parallelism) for i in range(self._parallelism)]
        pcoll |= beam.Create(slices)
        if self._reshuffle and self._parallelism > 1:
            pcoll |= beam.Reshuffle()
        return pcoll | beam.ParDo(self._dofn)


# noinspection PyAbstractClass
class _ElasticsearchScrollFn(beam.DoFn):
    def __init__(self,
                 es_args,
                 search_body,
                 search_size,
                 index,
                 search_args,
                 scroll_duration,
                 clear_scroll,
                 max_retries,
                 initial_backoff,
                 max_backoff,
                 request_timeout,
                 return_kv):
        super().__init__()

        self.es_args = es_args
        self.index = index
        self.search_body = search_body or {}
        self.search_size = search_size
        self.search_args = search_args or {}
        self.scroll_duration = scroll_duration
        self.clear_scroll = clear_scroll
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.request_timeout = request_timeout
        self.return_kv = return_kv
        self.client = None      # type: Elasticsearch | None

    def setup(self):
        self.client = Elasticsearch(**self.es_args)

    def teardown(self):
        if self.client:
            self.client.transport.close()
            self.client = None

    def _search(self, body):
        return self.client.search(index=self.index,
                                  body=body,
                                  scroll=self.scroll_duration,
                                  request_timeout=self.request_timeout,
                                  **self.search_args)

    def _scroll(self, scroll_id):
        return self.client.scroll(scroll_id=scroll_id,
                                  scroll=self.scroll_duration,
                                  request_timeout=self.request_timeout)

    def _clear_scroll(self, scroll_id):
        return self.client.clear_scroll(scroll_id=scroll_id,
                                        request_timeout=self.request_timeout)

    # noinspection PyMethodOverriding
    def process(self, element: t.Tuple[int, int]):
        slice_id, max_slices = element
        body = self.search_body.copy()
        if max_slices > 1:
            body['slice'] = dict(id=slice_id, max=max_slices)
        if 'size' not in body:
            body['size'] = self.search_size
        result = _retry_with_backoff(partial(self._search, body=body),
                                     self.max_retries, self.initial_backoff, self.max_backoff)

        scroll_id = None
        while result['hits']['hits']:
            scroll_id = result['_scroll_id']
            if self.return_kv:
                yield from ((hit['_id'], hit['_source']) for hit in result['hits']['hits'])
            else:
                yield from result['hits']['hits']
            result = _retry_with_backoff(partial(self._scroll, scroll_id=scroll_id),
                                         self.max_retries, self.initial_backoff, self.max_backoff)

        if scroll_id and self.clear_scroll:
            _retry_with_backoff(partial(self._clear_scroll, scroll_id=scroll_id),
                                self.max_retries, self.initial_backoff, self.max_backoff)
