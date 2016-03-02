
var _ = require('lodash')

exports.process = function(resp, commander) {

  var docs =  _.map(resp.hits.hits, function(hit) {
    return [
      {update: _.pick(hit, '_index', '_type', '_id')},
      {doc: hit._source, detect_noop: true, doc_as_upsert: true}
    ]
  })

  return commander.inputESClient.bulk({body: _.flatten(docs)})
}
