'use strict'

/*
 * This demonstrates producing & consuming topics against NYPL's kafka instance in Node
 */

var conf = require('./conf.json')
var kafka = require('kafka-node')

class KafkaClient {
  constructor (options) {
    options = options || {}
    options = Object.assign({ endpoint: conf.endpoint, client_id: conf.client_id }, options)
    this.client = new kafka.Client(options.endpoint, options.client_id)
  }
}

class KafkaProducer extends KafkaClient {
  constructor (options) {
    super(options)
    this.producer = new kafka.Producer(this.client)
  }

  produce (topic, messages) {
    this.producer.on('ready', () => {
      var payloads = [{ topic, messages }]

      var onComplete = function (err, data) {
        if (err) {
          console.log('ERROR: ', err)
        }
        console.log(`Published "${messages}" to ${topic}`)
        process.exit()
      }
      this.producer.send(payloads, onComplete)
    })
  }
}

function usage () {
  return 'Usage:' +
    '\n\tnode kafka-node-producer.js TOPIC MESSAGE'
}

function error (message) {
  console.log(message)
  process.exit()
}

var topic = process.argv[2]

if (!topic) error(usage())

var message = process.argv[3]
; (new KafkaProducer()).produce(topic, message)
