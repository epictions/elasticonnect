
var _ = require('lodash')

module.exports = function(resp, commander) {

  var docs =  _.map(resp.hits.hits, function(hit) {
    return [
      {update: _.pick(hit, '_index', '_type', '_id')},
      {doc: commander.extractDocument(hit), detect_noop: true, doc_as_upsert: true}
    ]
  })

  return commander.inputESClient.bulk({body: _.flatten(docs)})
}
