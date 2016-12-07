'use strict'

/*
 * This demonstrates producing & consuming topics against NYPL's kafka instance in Node
 */

var Kafka = require('no-kafka')

var common = require('./common')
var conf = require('./conf.json')

// no-kafka doesn't support zk?
conf.endpoint = conf.endpoint.replace(/2181/, '9092')

var consumer = new Kafka.SimpleConsumer({
  connectionString: conf.endpoint,
  recoveryOffset: Kafka.LATEST_OFFSET,
  startingOffset: Kafka.LATEST_OFFSET
})

var vals = {consumed: 0}

var dataHandler = function (messageSet, topic, partition) {
  messageSet.forEach(function (m) {
    if (vals.min == null) vals.min = m.offset
    vals.max = m.offset
    vals.consumed += 1
    common.report(vals)
  })
}

function consume (topic, offset, limit) {
  consumer.init().then(function () {
    var opts = offset ? { offset } : { time: Kafka.EARLIEST_OFFSET }
    return consumer.subscribe(topic, 0, opts, dataHandler)
  })
}

function usage () {
  return 'Usage:' +
    '\n\tnode no-kafka-v1.js TOPIC [OFFSET] [LIMIT]'
}

var topic = process.argv[2]

if (!topic) common.error(usage())

var offset = parseInt(process.argv[3])
var limit = parseInt(process.argv[4])

consume(topic, offset, limit)
