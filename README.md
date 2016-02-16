### Installation

You need elasticonnect installed globally:

```
$ npm i -g elasticonnect
```

### Usage

##### Delete by query
```
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type delete  '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
```

##### Update by query
```
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type --source=title,content --process-module=<./update-module.js> update '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
```

default update processing module is src/es-update.js

##### Dump by query
```
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type --source=title,content  --process-module=<./dump-module.js> dump --output=elasticsearch://localhost:9200/my_index2/my_type '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
```

default dump processing module is src/es-dump.js

### Development

```
$ git clone git@github.com:epictions/elasticonnect.git
$ cd elasticonnect
$ npm i
$ npm run build
$ DEBUG=* node dist -h
```

### Todos
Implement various connectors for dump command like mysql, postgresql, file, bigquery

License
----
ISC

**Free Software, Hell Yeah!**
