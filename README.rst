======
es2csv
======

A CLI tool for exporting data from Elasticsearch into a CSV file
----------------------------------------------------------------

Command line utility, written in Python, for querying Elasticsearch in Lucene query syntax or Query DSL syntax and exporting result as documents into a CSV file. This tool can query bulk docs in multiple indices and get only selected fields, this reduces query execution time.

Quick Look Demo
---------------
.. figure:: https://cloud.githubusercontent.com/assets/7491121/12016825/59eb5f82-ad58-11e5-81eb-871a49e39c37.gif

Requirements
------------
| This tool should be used with Elasticsearch 5.x version, for older version please check `2.x release <https://github.com/taraslayshchuk/es2csv/tree/2.x>`_.
| You also need `Python 2.7.x <https://www.python.org/downloads/>`_ and `pip <https://pip.pypa.io/en/stable/installing/>`_.

Installation
------------

From source:

.. code-block:: bash

    $ pip install git+https://github.com/taraslayshchuk/es2csv.git

From pip:

.. code-block:: bash

    $ pip install es2csv

Usage
-----
.. code-block:: bash

 $ es2csv [-h] -q QUERY [-u URL] [-a AUTH] [-i INDEX [INDEX ...]]
          [-D DOC_TYPE [DOC_TYPE ...]] [-t TAGS [TAGS ...]] -o FILE
          [-f FIELDS [FIELDS ...]] [-S FIELDS [FIELDS ...]] [-d DELIMITER]
          [-m INTEGER] [-s INTEGER] [-k] [-r] [-e] [--verify-certs]
          [--ca-certs CA_CERTS] [--client-cert CLIENT_CERT]
          [--client-key CLIENT_KEY] [-v] [--debug]

 Arguments:
  -q, --query QUERY                        Query string in Lucene syntax.               [required]
  -o, --output-file FILE                   CSV file location.                           [required]
  -u, --url URL                            Elasticsearch host URL. Default is http://localhost:9200.
  -a, --auth                               Elasticsearch basic authentication in the form of username:password.
  -i, --index-prefixes INDEX [INDEX ...]   Index name prefix(es). Default is ['logstash-*'].
  -D, --doc-types DOC_TYPE [DOC_TYPE ...]  Document type(s).
  -t, --tags TAGS [TAGS ...]               Query tags.
  -f, --fields FIELDS [FIELDS ...]         List of selected fields in output. Default is ['_all'].
  -S, --sort FIELDS [FIELDS ...]           List of <field>:<direction> pairs to sort on. Default is [].
  -d, --delimiter DELIMITER                Delimiter to use in CSV file. Default is ",".
  -m, --max INTEGER                        Maximum number of results to return. Default is 0.
  -s, --scroll-size INTEGER                Scroll size for each batch of results. Default is 100.
  -k, --kibana-nested                      Format nested fields in Kibana style.
  -r, --raw-query                          Switch query format in the Query DSL.
  -e, --meta-fields                        Add meta-fields in output.
  --verify-certs                           Verify SSL certificates. Default is False.
  --ca-certs CA_CERTS                      Location of CA bundle.
  --client-cert CLIENT_CERT                Location of Client Auth cert.
  --client-key CLIENT_KEY                  Location of Client Cert Key.
  -v, --version                            Show version and exit.
  --debug                                  Debug mode on.
  -h, --help                               show this help message and exit

[ `Usage Examples <./docs/EXAMPLES.rst>`_ | `Release Changelog <./docs/HISTORY.rst>`_ ]

Func Reference
--------------
.. code-block:: python

    from es2csv_lib import Es2csv


    es = Es2csv(*args) # 自定义传入参数
    es.export_csv()

Func Params
-----------
.. code-block:: python

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
