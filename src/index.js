
var _ = require('lodash')
var Q = require('q')
var async = require('async-q')
var elasticsearch = require('elasticsearch')
var commander = require('commander')
var fs = require('fs')
var ProgressBar = require('progress')
var inquirer = require('inquirer')
var URL = require('url')

var debug = require('debug')('elasticonnect')
var error =  require('debug')('elasticonnect')
error.log = console.error.bind(console)

function scanAndScroll(response) {
  var stop = false
  var scrollId = response._scroll_id

  var tobeProcessed = commander.max && Math.min(commander.max, response.hits.total) || response.hits.total
  var progressPrompt =  '  ' + (commander.task === 'delete' ? 'deleting' : 'dumping') + ' [:bar] :percent :current/:total :eta'

  if (!tobeProcessed) {
    return Q()
  }

  debug('Total', tobeProcessed)
  var bar = new ProgressBar(progressPrompt, {
    complete: '=',
    incomplete: ' ',
    width: 60,
    total: +tobeProcessed
  });


  return Q.fcall(function() {
    if (commander.quiet) {
      return
    }
    var deferred = Q.defer()
    inquirer.prompt({
      type: 'confirm',
      name: 'answer',
      message: 'Are you sure to ' + commander.task + ' ' + tobeProcessed + ' documents'
    }, function(answers) {
      if (!answers.answer) {
        process.exit(0)
      }
      deferred.resolve()
    })
    return deferred.promise
  })
  .then(function() {
    bar.tick(0)
    return async.until(() => stop, function() {
      return commander.inputESClient.scroll({scrollId: scrollId, scroll: '1m'})
      .then(function(resp) {
        scrollId = resp._scroll_id

        if (resp.hits.hits.length) {
          return Q.fcall(function() {
            return commander.process(resp, commander)
          })
          .then(function() {
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
    commander.inputESClient.clearScroll({scrollId: scrollId})
  })
}

function main() {
  debug('Executing main')

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

  return commander.inputESClient.search(query)
  .then(scanAndScroll)
  .catch(function(ex) {
    if (/not found/i.test(ex.message)) {
      debug('Processed successfully')
    }
    else {
      error(ex)
      debug('Failed')
    }
  })
  .then(function() {
    if (commander.outputStream) {
      commander.outputStream.close()
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

    if (type == 'input' && !(commander.inputIndex && commander.inputType)) {
      throw new Error('Missing elasticsearch index name or index type')
    }

    var hosts = _.map(arr, function(uri) {
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
    if (!commander[type + 'Format']) {
      throw new Error('Missing ' + type + ' file format for file uri')
    }

    if (type === 'output' && arr.length !== 1) {
      throw new Error('Output doesnt support multiple files')
    }

    var streamType = (type === 'input') ? 'Read' : 'Write'
    var streams = commander[type + 'Streams'] = []

    streams.push(_.map(arr, (file) => fs['create' + streamType + 'Stream'](file.replace('file://'))))
  }
}

function common(query, options) {

  if (options) {
    _.extend(commander, options)
  }

  debug('input', commander.input)

  try {
    createStreams(commander.input, 'input')
    if (commander.output) {
      createStreams(commander.output, 'output')
    }
    else if (commander.task = 'dump') {
      createStreams(commander.input, 'output')
    }
  }
  catch (ex) {
    error(ex)
    process.exit(0)
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
        commander.process = require('./es-delete.js')
      }
      else {
        commander.process = require(commander.processModule)
      }
    }
    else {
      throw new Error('not yet supported delete for uri ' + commander.inputUriType)
    }
  }
  else if (commander.task === 'update') {
    if (commander.inputUriType === 'elasticsearch') {
      if (!commander.processModule) {
        commander.process = require('./es-update.js')
      }
      else {
        commander.process = require(commander.processModule)
      }
    }
    else {
      throw new Error('not yet supported update for uri ' + commander.inputUriType)
    }
  }
  else if (commander.task === 'dump') {
    if (commander.inputUriType === 'elasticsearch' && commander.outputUriType === 'elasticsearch') {
      if (!commander.outputIndex) {
        commander.outputIndex = commander.inputIndex
      }

      if (!commander.outputType) {
        commander.outputType = commander.inputType
      }

      if (!commander.processModule) {
        commander.process = require('./es-dump.js')
      }
      else {
        commander.process = require(commander.processModule)
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
  .version('0.0.1')
  .option('--input <input>', 'input uris, elasticsearch client nodes supports list, '
          + 'example: elasticsearch://localhost:9200/my_index/my_type,http://192.168.0.1:9200/my_index/my_type', list)
  .option('--quiet', 'dont ask for confirmation')
  .option('--input-index [inputIndex]', 'input elasticsearch index')
  .option('--input-type [inputType]', 'input elasticsearch type')
  .option('--input-format [inputFormat]', 'if input is file uri, format of the file: JSON, LINE_DELIMETED_JSON, CSV')
  .option('--scroll [scroll]', 'scroll context timeout, default: 1m')
  .option('--size [size]', 'number of documents to fetch at a time, default: 50')
  .option('--max [max]', 'max number of documents to process, default: all')
  .option('--query-file [queryFile]', 'File to read the query from')
  .option('--source [source]', 'list of fields to fetch, default: all example: title,content', list)

commander
  .command('delete [query]')
  .description('delete documents by query or query file')
  .action(function(query) {
    commander.task = 'delete'
    commander._source = false
    try {
      common(query)
    } catch (ex) {
      error(ex)
    }
  })

commander
  .command('update [query]')
  .description('update documents by query')
  .option('--process-module <processModule>', 'module to manipulate the input documents,'
          + 'signature: module.exports = function(resp, commander) {}')
  .action(function(query) {
    commander.task = 'update'
    common(query)
  })

commander
  .command('dump [query]')
  .description('dump documents by query')
  .option('--output [output]', 'list of elasticsearch client nodes, '
          + 'example: elasticsearch://localhost:9200,http://192.168.0.1:9200', list)
  .option('--output-index [outputIndex]', 'output elasticsearch index')
  .option('--output-type [outputType]', 'output elasticsearch type')
  .option('--output-format [outputFormat]', 'if output is file uri, format of the file: JSON, LINE_DELIMETED_JSON, CSV')
  .option('--process-module [processModule]', 'module to manipulate the input documents,  '
          + 'signature: module.exports = function(resp, commander) {}')
  .action(function(query, options) {
    commander.task = 'dump'
    common(query, options)
  })

commander.parse(process.argv)

