'use strict'

/*
 * This demonstrates producing & consuming topics against NYPL's kafka instance in Node
 */

var kafka = require('kafka-node')

var conf = require('./conf.json')
var common = require('./common')

var Consumer = kafka.Consumer

class KafkaClient {
  constructor (options) {
    options = options || {}
    options = Object.assign({ endpoint: conf.endpoint, client_id: conf.client_id }, options)
    this.client = new kafka.Client(options.endpoint, options.client_id)
  }
}

var vals = {consumed: 0}

class KafkaConsumer extends KafkaClient {

  consume (topic, offset, limit) {
    var payload = { topic, partition: 0, offset }
    this.consumer = new Consumer(this.client, [payload], {groupId: conf.group_id, autoCommit: false, fromOffset: true})
    var consumed = 0
    this.consumer.on('message', function (m) {
      consumed += 1
      if (limit && consumed === limit) process.exit()

      if (vals.min == null) vals.min = m.offset
      vals.max = m.offset
      vals.consumed += 1
      common.report(vals)
    }).on('error', function (err) {
      console.log('ERROR: ', err)
    })
  }
}

function usage () {
  return 'Usage:' +
    '\n\tnode kafka-node-v1.js TOPIC [OFFSET] [LIMIT]'
}

var topic = process.argv[2]

if (!topic) common.error(usage())

var offset = parseInt(process.argv[3])
var limit = parseInt(process.argv[4])
; (new KafkaConsumer()).consume(topic, offset, limit)

