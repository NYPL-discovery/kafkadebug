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
    options = Object.assign({ endpoint: conf.zk_endpoint, client_id: conf.client_id }, options)
    this.client = new kafka.Client(options.endpoint, options.client_id)
  }
}

var vals = {consumed: 0}

class KafkaConsumer extends KafkaClient {

  consume (topic, offset, limit) {
    var payload = { topic, partition: 0, offset }
    this.consumer = new Consumer(this.client, [payload], {autoCommit: false, fromOffset: true})
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

  offsets (topic) {
    var offset = new kafka.Offset(this.client)
    // time: -1 fetches "latest" offset(s) whereas time: -2 fetches "earliest", so have to issue both calls and merge:
    return Promise.all(
      [-1, -2].map((time) => {
        return new Promise((resolve, reject) => {
          offset.fetch([{ topic, partition: 0, time, maxNum: 2 }], (err, data) => {
            if (err) reject(err)
            else {
              resolve(data[topic][0].map((v) => parseInt(v)))
            }
          })
        })
      })
    ).then((offsets) => {
      offsets = [].concat.apply([], offsets)
      console.log('got offsets: ', offsets)
      return {
        min: Math.min.apply(null, offsets),
        max: Math.max.apply(null, offsets)
      }
    })
  }
}

function usage () {
  return 'Usage:' +
    '\n\tnode kafka-node-v1.js TOPIC [OFFSET] [LIMIT]'
}

var topic = process.argv[2]

if (!topic) common.error(usage())
topic

var offset = parseInt(process.argv[3])
var limit = parseInt(process.argv[4])

var consumer = new KafkaConsumer()
if (!offset) {
  consumer.offsets(topic).then((offsets) => {
    offset = offsets.min
    limit = offsets.max

    consumer.consume(topic, offset, limit)
  })
} else {
  consumer.consume(topic, offset, limit)
}

