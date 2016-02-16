### Installation

You need elasticonnect installed globally:

``sh
$ npm i -g elasticonnect
``

### Usage

#### Delete by query
``sh
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type delete  '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
``
#### Update by query
``sh
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type update --source=title,content --process-module=<./update-module.js> '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
``

#### Dump by query
``sh
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type dump --source=title,content  --process-module=<./update-module.js> --output=elasticsearch://localhost:9200/my_index2/my_type '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
``


### Development

``sh
$ git clone git@github.com:epictions/elasticonnect.git
$ cd elasticonnect
$ npm i
$ npm run build
$ DEBUG=* node dist -h
``

### Todos
Implement various connectors for dump command like mysql, postgresql, file, bigquery

License
----
ISC

**Free Software, Hell Yeah!**
