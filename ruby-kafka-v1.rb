require "kafka"
require 'whirly'

require 'json'

conf = JSON.parse(File.read('./conf.json'))

kafka = Kafka.new(seed_brokers: [conf['endpoint']])

vals = {}
vals[:consumed] = 0

Whirly.start do
  kafka.each_message(topic: "Bib") do |m|
    vals[:min] = m.offset if (vals[:min].nil?) 
    vals[:max] = m.offset
    vals[:consumed] += 1
    Whirly.status = vals.map { |(k,v)| "#{k}: #{v}" }.join("\t")
  end
end

puts "done"
