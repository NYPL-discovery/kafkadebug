require "kafka"
require 'whirly'

require 'json'

conf = JSON.parse(File.read('./conf.json'))

puts 'Topic: ', ARGV[0]
kafka = Kafka.new(seed_brokers: [conf['endpoint']])
consumer = kafka.consumer(group_id: conf['group_id'])
consumer.subscribe(ARGV[0], start_from_beginning: true)

vals = {}
vals[:consumed] = 0

Whirly.start do
  consumer.each_message do |m|
    vals[:min] = m.offset if (vals[:min].nil?) 
    vals[:max] = m.offset
    vals[:consumed] += 1
    Whirly.status = vals.map { |(k,v)| "#{k}: #{v}" }.join("\t")
  end
end

puts "done"
