
var _ = require('lodash')

module.exports = function(resp, commander) {

  var docs =  _.map(resp.hits.hits, function(hit) {
    return [
      {update: {_index: commander.outputIndex, _type: commander.outputType, _id: hit._id}},
      {doc: hit._source, detect_noop: true, doc_as_upsert: true}
    ]
  })

  return commander.outputESClient.bulk({body: _.flatten(docs)})
}
