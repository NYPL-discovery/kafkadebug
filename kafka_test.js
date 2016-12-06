'use strict'

/*
 * This demonstrates producing & consuming topics against NYPL's kafka instance in Node
 */

var kafka = require('kafka-node')
var Consumer = kafka.Consumer

class KafkaClient {
    constructor (options) {
        options = options || {}
        // options = Object.assign({ endpoint: '54.159.172.103:2181/', client_id: 'kafka-node-client' }, options)
        options = Object.assign({ endpoint: 'kafka.nypltech.org:2181/', client_id: 'kafka-node-client' }, options)
        // options = Object.assign({ endpoint: 'kafka.nypltech.org:9092/', client_id: 'kafka-node-client-fooo' }, options)
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
            }

            this.producer.send(payloads, onComplete)
    })
    }
}

class KafkaConsumer extends KafkaClient {

    consume (topic, offset, limit) {
        // this.consumer = new Consumer(this.client, [{topic, offset}], {groupId: 'kafka-node-group-mine', autoCommit: false, fromOffset: false})
        this.consumer = new Consumer(this.client, [{topic: topic, offset: offset}], {groupId: 'kafka-node-group', autoCommit: false, fromOffset: true})

        var consumed = 0
        this.consumer.on('message', function (message) {
            console.log(`${topic}[${message.offset}(${message.partition})]: ${message.value}`)
            consumed += 1
            if (limit && consumed === limit) process.exit()
        }).on('error', function (err) {
            console.log('ERROR: ', err)
        }).on('offsetOutOfRange', function (err) {
            this.setOffset(topic, 0, ++offset)
            console.log('offset out of range: ' + offset, err)
        })
    }
}

function usage () {
    return 'Usage:' +
        '\n\tnode kafka_test.js producer TOPIC MESSAGE' +
        '\n\tnode kafka_test.js consumer TOPIC [OFFSET]'
}

function error (message) {
    console.log(message)
    process.exit()
}

var mode = process.argv[2] || 'consume'
var topic = process.argv[3]

if (!mode || !topic) error(usage())

if (mode === 'consumer') {
    var offset = parseInt(process.argv[4])
    var limit = parseInt(process.argv[5])
        ; (new KafkaConsumer()).consume(topic, offset, limit)
} else if (mode === 'producer') {
    var message = process.argv[4]
        ; (new KafkaProducer()).produce(topic, message)
}
