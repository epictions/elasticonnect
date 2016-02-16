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

##### Dump by query
```
$ DEBUG=* elasticonnect --input=elasticsearch://localhost:9200/my_index/my_type --source=title,content  --process-module=<./update-module.js> dump --output=elasticsearch://localhost:9200/my_index2/my_type '{"query":{"range":{"publish_time":{"gte":1262304000000}}}}'
```


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
