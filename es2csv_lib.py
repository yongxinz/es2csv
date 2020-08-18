import os
import time
import json
import codecs
from functools import wraps

import elasticsearch
from backports import csv


FLUSH_BUFFER = 1000  # Chunk of docs to flush in temp file
CONNECTION_TIMEOUT = 120
TIMES_TO_TRY = 3
RETRY_DELAY = 60
META_FIELDS = [u'_id', u'_index', u'_score', u'_type']


# Retry decorator for functions with exceptions
def retry(ExceptionToCheck, tries=TIMES_TO_TRY, delay=RETRY_DELAY):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries = tries
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    print(e)
                    print('Retrying in {} seconds ...'.format(delay))
                    time.sleep(delay)
                    mtries -= 1
                else:
                    print('Done.')
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print('Fatal Error: {}'.format(e))
                exit(1)

        return f_retry

    return deco_retry


class Es2csv:

    def __init__(self, opts):
        self.opts = opts
        self.url = self.opts.get('url', '')
        self.auth = self.opts.get('auth', '')
        self.index_prefixes = self.opts.get('index_prefixes', [])
        self.sort = self.opts.get('sort', [])
        self.fields = self.opts.get('fields', [])
        self.query = self.opts.get('query', {})
        self.tags = self.opts.get('tags', [])
        self.output_file = self.opts.get('output_file', 'export.csv')
        self.raw_query = self.opts.get('raw_query', True)
        self.delimiter = self.opts.get('delimiter', ',')
        self.max_results = self.opts.get('max_results', 0)
        self.scroll_size = self.opts.get('scroll_size', 100)
        self.meta_fields = self.opts.get('meta_fields', [])
        self.debug_mode = self.opts.get('debug_mode', False)

        self.num_results = 0
        self.scroll_ids = []
        self.scroll_time = '30m'

        self.csv_headers = list(META_FIELDS) if self.opts['meta_fields'] else []
        self.tmp_file = '{}.tmp'.format(self.output_file)

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def create_connection(self):
        es = elasticsearch.Elasticsearch(self.url, timeout=CONNECTION_TIMEOUT, http_auth=self.auth)
        es.cluster.health()
        self.es_conn = es

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def check_indexes(self):
        indexes = self.index_prefixes
        if '_all' in indexes:
            indexes = ['_all']
        else:
            indexes = [index for index in indexes if self.es_conn.indices.exists(index)]
            if not indexes:
                print('Any of index(es) {} does not exist in {}.'.format(', '.join(self.index_prefixes), self.url))
                exit(1)
        self.index_prefixes = indexes

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def search_query(self):
        @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
        def next_scroll(scroll_id):
            return self.es_conn.scroll(scroll=self.scroll_time, scroll_id=scroll_id)

        search_args = dict(
            index=','.join(self.index_prefixes),
            sort=','.join(self.sort),
            scroll=self.scroll_time,
            size=self.scroll_size,
            terminate_after=self.max_results
        )

        if self.raw_query:
            query = self.query
            search_args['body'] = self.query
        else:
            query = self.query if not self.tags else '{} AND tags: ({})'.format(
                self.query, ' AND '.join(self.tags))
            search_args['q'] = query

        if '_all' not in self.fields:
            search_args['_source'] = True
            search_args['_source_includes'] = ','.join(self.fields)
            self.csv_headers.extend([field for field in self.fields if '*' not in field])

        if self.debug_mode:
            print('Using these indices: {}.'.format(', '.join(self.index_prefixes)))
            print('Query[{0[0]}]: {0[1]}.'.format(
                ('Query DSL', json.dumps(query, ensure_ascii=False).encode('utf8')) if self.raw_query else ('Lucene', query))
            )
            print('Output field(s): {}.'.format(', '.join(self.fields)))
            print('Sorting by: {}.'.format(', '.join(self.sort)))

        res = self.es_conn.search(**search_args)
        self.num_results = res['hits']['total']

        print('Found {} results.'.format(self.num_results))
        if self.debug_mode:
            print(json.dumps(res, ensure_ascii=False).encode('utf8'))

        if self.num_results > 0:
            codecs.open(self.output_file, mode='w', encoding='utf-8').close()
            codecs.open(self.tmp_file, mode='w', encoding='utf-8').close()

            hit_list = []
            total_lines = 0

            while total_lines != self.num_results:
                if res['_scroll_id'] not in self.scroll_ids:
                    self.scroll_ids.append(res['_scroll_id'])

                if not res['hits']['hits']:
                    print('Scroll[{}] expired(multiple reads?). Saving loaded data.'.format(res['_scroll_id']))
                    break
                for hit in res['hits']['hits']:
                    total_lines += 1
                    hit_list.append(hit)
                    if len(hit_list) == FLUSH_BUFFER:
                        self.flush_to_file(hit_list)
                        hit_list = []
                    if self.opts['max_results']:
                        if total_lines == self.opts['max_results']:
                            self.flush_to_file(hit_list)
                            print('Hit max result limit: {} records'.format(self.opts['max_results']))
                            return
                res = next_scroll(res['_scroll_id'])
            self.flush_to_file(hit_list)

    def flush_to_file(self, hit_list):
        def to_keyvalue_pairs(source, ancestors=[], header_delimeter='.'):
            def is_list(arg):
                return type(arg) is list

            def is_dict(arg):
                return type(arg) is dict

            if is_dict(source):
                for key in source.keys():
                    to_keyvalue_pairs(source[key], ancestors + [key])

            elif is_list(source):
                [to_keyvalue_pairs(item, ancestors + [str(index)]) for index, item in enumerate(source)]
            else:
                header = header_delimeter.join(ancestors)
                if header not in self.csv_headers:
                    self.csv_headers.append(header)
                try:
                    out[header] = '{}{}{}'.format(out[header], self.opts.delimiter, source)
                except:
                    out[header] = source

        with codecs.open(self.tmp_file, mode='a', encoding='utf-8') as tmp_file:
            for hit in hit_list:
                out = {field: hit[field] for field in META_FIELDS} if self.opts['meta_fields'] else {}
                if '_source' in hit and len(hit['_source']) > 0:
                    to_keyvalue_pairs(hit['_source'])
                    tmp_file.write('{}\n'.format(json.dumps(out)))
        tmp_file.close()

    def write_to_csv(self):
        if self.num_results > 0:
            self.num_results = sum(1 for line in codecs.open(self.tmp_file, mode='r', encoding='utf-8'))
            if self.num_results > 0:
                output_file = codecs.open(self.output_file, mode='a', encoding='utf-8')
                csv_writer = csv.DictWriter(output_file, fieldnames=self.csv_headers)
                csv_writer.writeheader()
                timer = 0

                for line in codecs.open(self.tmp_file, mode='r', encoding='utf-8'):
                    timer += 1
                    csv_writer.writerow(json.loads(line))
                output_file.close()
            else:
                print('There is no docs with selected field(s): {}.'.format(','.join(self.opts['fields'])))
            os.remove(self.tmp_file)

    def clean_scroll_ids(self):
        try:
            self.es_conn.clear_scroll(body=','.join(self.scroll_ids))
        except:
            pass

    def export_csv(self):
        self.create_connection()
        self.check_indexes()
        self.search_query()
        self.write_to_csv()
        self.clean_scroll_ids()
