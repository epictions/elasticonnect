
var _ = require('lodash')

exports.process = function(resp, commander) {
  var docs =  _.map(resp.hits.hits, function(hit) {
    return {delete: _.pick(hit, '_index', '_type', '_id')}
  })

  return commander.inputESClient.bulk({body: docs})
}
