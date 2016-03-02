
const _ = require('lodash')
const Q = require('q')
const async = require('async-q')
const elasticsearch = require('elasticsearch')
const commander = require('commander')
const fs = require('fs')
const ProgressBar = require('progress')
const inquirer = require('inquirer')
const URL = require('url')
const readline = require('readline')

const debug = require('debug')('elasticonnect')
const error =  require('debug')('elasticonnect')
error.log = console.error.bind(console)

function scanAndScroll(response) {
  var stop = false
  var scrollId = response._scroll_id

  const tobeProcessed = commander.max && Math.min(commander.max, response.hits.total) || response.hits.total
  const progressPrompt =  '  ' + (commander.task === 'delete' ? 'deleting' : 'dumping') + ' [:bar] :percent :current/:total :eta'

  if (!tobeProcessed) {
    return Q()
  }

  debug('Total', tobeProcessed)
  const bar = new ProgressBar(progressPrompt, {
    complete: '=',
    incomplete: ' ',
    width: 60,
    total: +tobeProcessed
  });

  commander.processModule.start(commander)

  return Q.fcall(() => {
    if (commander.quiet) {
      return
    }

    const deferred = Q.defer()
    inquirer.prompt({
      type: 'confirm',
      name: 'answer',
      message: 'Are you sure to ' + commander.task + ' ' + tobeProcessed + ' documents'
    }, (answers) => {
      if (!answers.answer) {
        process.exit(0)
      }
      deferred.resolve()
    })
    return deferred.promise
  })
  .then(() => {
    bar.tick(0)
    return async.until(() => stop, () => {
      return commander.inputESClient.scroll({scrollId: scrollId, scroll: '1m'})
      .then((resp) => {
        scrollId = resp._scroll_id

        if (resp.hits.hits.length) {
          return Q.fcall(() => {
            return commander.processModule.process(resp, commander)
          })
          .then(() => {
            bar.tick(resp.hits.hits.length)
            if (bar.complete) {
              stop = true
            }
          })
        }
        else {
          stop = true
        }
      })
    })
  })
  .then(() => {
    commander.processModule.end(commander)
    commander.inputESClient.clearScroll({scrollId: scrollId})
  })
}

function main() {
  debug('Executing main', commander.inputUriType)

  return Q.fcall(() => {
    if (commander.inputUriType === 'file') {
      return async.eachSeries(commander.inputFileStreams, (stream) => {
        var chunk
        var json
        var lines = []
        var deferred = Q.defer()
        var buffer = ''

        if (commander.inputFileFormat === 'LINE_DELIMETED_JSON') {
          const rl = readline.createInterface({
            input: stream
          })

          rl.on('line', (line) => {
            lines.push(JSON.parse(line))
            if (lines.length >= commander.size) {
              stream.pause()
              return commander.processModule.process(lines, commander)
              .then(() => {
                lines = []
                stream.resume()
              })
            }
          })

          rl.on('close', () => {
            if (lines.length) {
              return commander.processModule.process(json, commander)
              .then(deferred.resolve)
            }
            deferred.resolve()
          })
        }
        else if (commander.inputFileFormat === 'JSON') {
          stream.on('data', (chunk) => {
            stream.pause()

            chunk = buffer + chunk
            chunk = chunk.trim().replace(/^\[/, '')

            var i = chunk.length - 1
            while (true) {
              while ((chunk[i] != ',' && chunk[i] != ']') && i > 0) {
                i--
              }
              try {
                chunk = '[' + chunk.substr(0, i) + ']'
                json = JSON.parse(chunk)
                buffer = chunk.substr(i + 1)
                break;
              }
              catch (ex) {
              }
            }

            if (json) {
              return commander.processModule.process(json, commander)
              .then(() => stream.resume())
            }
            else {
              stream.resume()
            }
          })
          stream.on('end', () => deferred.resolve)
          stream.on('error', () => deferred.reject)
        }
        return deferred.promise
      })
    }
    else if (commander.inputUriType === 'elasticsearch') {
      var query = {
        index: commander.inputIndex,
        type: commander.inputType,
        scroll: commander.scroll || '1m',
        searchType: 'scan',
        queryCache: true,
        sort: ['_doc'],
        size: commander.size || 50,
        body: commander.query
      }

      if (commander.source) {
        query._source = commander.source
      }

      if (commander.sourceExclude) {
        query._sourceExclude = commander.sourceExclude
      }

      if (commander.sourceInclude) {
        query._sourceInclude = commander.sourceInclude
      }

      return commander.inputESClient.search(query)
      .then(scanAndScroll)
      .catch((ex) => {
        if (/not found/i.test(ex.message)) {
          debug('Processed successfully')
        }
        else {
          error(ex)
          debug('Failed')
        }
      })
    }
  })
  .then(() => {
    if (commander.outputFileStream) {
      commander.outputFileStream.close()
    }
    process.exit(0)
  })
}

function createStreams(arr, type) {
  var uriType = commander[type + 'UriType'] = arr[0].split(':')[0]

  if (uriType === 'elasticsearch') {

    var u = URL.parse(arr[0])

    if (u.pathname) {
      var pathComponents = u.pathname.replace(/^\/|\/$/g, '').split('/')

      if (pathComponents.length >= 1) {
        commander[type + 'Index'] = pathComponents[0]
      }

      if (pathComponents.length >= 2) {
        commander[type + 'Type'] = pathComponents[1]
      }
    }

    var hosts = _.map(arr, (uri) => {
      u = URL.parse(uri)
      u.protocol = 'http'
      u.pathname = '/'

      return u.format()
    })

    debug('hosts', hosts)

    commander[type + 'ESClient'] = new elasticsearch.Client({
      hosts: hosts,
      requestTimeout: 60000
    })
  }
  else if (uriType === 'file') {
    if (type === 'output') {
      if (arr.length !== 1) {
        throw new Error('Output doesnt support multiple files')
      }
      else {
        commander.outputFileStream = fs.createWriteStream(arr[0].replace('file://', ''))
      }

      if (!commander.outputFileFormat) {
        commander.outputFileFormat = 'LINE_DELIMETED_JSON'
      }
    }
    else {
      commander.inputFileStreams = _.map(arr, (file) => {
        file = file.replace('file://', '')
        return fs.createReadStream(file, {
          encoding: 'utf8',
        })
      })

      if (!commander.inputFileFormat) {
        commander.inputFileFormat = 'LINE_DELIMETED_JSON'
      }
    }
  }
}

function parseOptions(query, options) {

  if (options) {
    _.extend(commander, options)
  }

  if (!commander.input && !commander.inputConfig) {
    throw new Error('--input or --input-config required')
  }

  if (commander.input) {
    debug('input', commander.input)

    try {
      createStreams(commander.input, 'input')
    }
    catch (ex) {
      error(ex)
      process.exit(0)
    }
  }
  else {
    commander.inputUriType = 'elasticsearch'
    commander['inputESClient'] = new elasticsearch.Client(require(commander.inputConfig))
  }

  if (commander.output) {
    debug('output', commander.output)

    try {
      createStreams(commander.output, 'output')
    }
    catch (ex) {
      error(ex)
      process.exit(0)
    }
  }
  else if (commander.outputConfig) {
    commander.outputUriType = 'elasticsearch'
    commander['outputESClient'] = new elasticsearch.Client(require(commander.outputConfig))
  }
  else if (commander.task == 'dump') {
    createStreams(commander.input, 'output')
  }

  if (commander.inputUriType === 'elasticsearch' && !(commander.inputIndex && commander.inputType)) {
    throw new Error('Missing elasticsearch input index name or input index type')
  }

  if ((commander.task === 'delete' || commander.task === 'update') && commander.inputUriType === 'file') {
    throw new Error('delete/update doesnt support for file')
  }

  if (commander.inputUriType === 'elasticsearch') {
    if (commander.queryFile) {
      commander.query = require(commander.queryFile)
    }
    else {
      commander.query = query ? JSON.parse(query) : {query: {match_all: {}}}
    }
  }
  else {
    if (commander.queryFile) {
      commander.query = fs.readFileSync(commander.queryFile).trim()
    }
    else {
      commander.query = query
    }
  }

  if (commander.task === 'delete') {
    if (commander.inputUriType === 'elasticsearch') {
      if (!commander.processModule) {
        commander.processModule = require('./es-delete.js')
      }
      else {
        commander.processModule = require(commander.processModule)
      }
    }
    else {
      throw new Error('not yet supported delete for uri ' + commander.inputUriType)
    }
  }
  else if (commander.task === 'update') {
    if (commander.inputUriType === 'elasticsearch') {
      if (!commander.processModule) {
        commander.processModule = require('./es-update.js')
      }
      else {
        commander.processModule = require(commander.processModule)
      }
    }
    else {
      throw new Error('not yet supported update for uri ' + commander.inputUriType)
    }
  }
  else if (commander.task === 'dump') {
    if (commander.inputUriType === 'elasticsearch') {
      if (commander.outputUriType === 'elasticsearch') {
        if (!commander.outputIndex) {
          commander.outputIndex = commander.inputIndex
        }

        if (!commander.outputType) {
          commander.outputType = commander.inputType
        }

        if (!commander.processModule) {
          commander.processModule = require('./es-dump.js')
        }
        else {
          commander.processModule = require(commander.processModule)
        }
      }
      else if (commander.outputUriType === 'file') {
        if (!commander.processModule) {
          commander.processModule = require('./fs-dump.js')
        }
        else {
          commander.processModule = require(commander.processModule)
        }
      }
    }
    else if (commander.inputUriType === 'file') {
      if (commander.outputUriType === 'elasticsearch') {
        if (!commander.outputIndex && !commander.outputType) {
          throw new Error('OutputIndex and OuputType not set')
        }

        if (!commander.processModule) {
          commander.processModule = require('./file-es-dump.js')
        }
        else {
          commander.processModule = require(commander.processModule)
        }
      }
      else {
        throw new Error('not yet supported dump for input uri ' + commander.inputUriType + ' output uri ' + commander.outputUriType)
      }
    }
    else {
      throw new Error('not yet supported dump for input uri ' + commander.inputUriType + ' output uri ' + commander.outputUriType)
    }
  }

  debug('task', commander.task)
  main()
}


function list(val) {
  return val.split(',').map(s => s.trim())
}

commander
  .version('0.0.7')
  .option('--input [input]', 'input uris, elasticsearch client nodes supports list, '
          + 'example: elasticsearch://localhost:9200/my_index/my_type,http://192.168.0.1:9200/my_index/my_type or file://dump.json', list)
  .option('--input-config [inputConfig]', 'input elasticsearch configuration module, '
          + 'https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html')
  .option('--quiet', 'dont ask for confirmation')
  .option('--input-index [inputIndex]', 'input elasticsearch index')
  .option('--input-type [inputType]', 'input elasticsearch type')
  .option('--input-file-format [inputFileFormat]', 'if input is file uri, format of the file: JSON, LINE_DELIMETED_JSON, CSV')
  .option('--scroll [scroll]', 'scroll context timeout, default: 1m')
  .option('--size [size]', 'number of documents to fetch at a time, default: 50')
  .option('--max [max]', 'max number of documents to process, default: all')
  .option('--query-file [queryFile]', 'file to read the query from')
  .option('--process-module [processModule]', 'module to manipulate the input documents,'
          + 'example: src/es-dump.js')
  .option('--source [source]', 'list of fields to fetch, default: all example: title,content', list)
  .option('--sourceExclude [sourceExclude]', 'list of fields to exclude, example: title,content', list)
  .option('--sourceInclude [sourceInclude]', 'list of fields to include, example: title,content', list)

commander
  .command('delete [query]')
  .description('delete documents by query or query file')
  .action(function(query) {
    commander.task = 'delete'
    commander._source = false
    try {
      parseOptions(query)
    } catch (ex) {
      error(ex)
    }
  })

commander
  .command('update [query]')
  .description('update documents by query')
  .action((query) => {
    commander.task = 'update'
    parseOptions(query)
  })

commander
  .command('dump [query]')
  .description('dump documents by query')
  .option('--output [output]', 'list of elasticsearch client nodes, '
          + 'example: elasticsearch://localhost:9200,http://192.168.0.1:9200 or file://dump.json', list)
  .option('--output-config [outputConfig]', 'output elasticsearch configuration module, '
          + 'https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html')
  .option('--output-index [outputIndex]', 'output elasticsearch index')
  .option('--output-type [outputType]', 'output elasticsearch type')
  .option('--output-file-format [outputFileFormat]', 'if output is file uri, format of the file: JSON, LINE_DELIMETED_JSON, CSV, default: LINE_DELIMETED_JSON')
  .action((query, options) => {
    commander.task = 'dump'
    parseOptions(query, options)
  })

commander.parse(process.argv)

