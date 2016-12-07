# kafkadebug

tldr: Using two different node clients and a ruby client, it's looking like the lowest offset in Bib is 1639354 and there are only about 202059 entries in that topic currently. Is that the case?

## no-kafka

```
npm install no-kafka
node no-kafka-v1.js Bib
```

Result:

```
consumed: 202059	min: 1639354	max: 1841412 
```
W/out an explicit offset, consumes everything from 1639354 to the (then) final offset of the topic.

## kafka-node

```
npm install kafka-node
```

### v1

```
export DEBUG=kafka-node:*
node kafka-node-v1.js Bib
```

Result:

: (

### v2

```
export DEBUG=kafka-node:*
node kafka-node-v1.js Bib
```

Result:

```
consumed: 202079	min: 1639354	max: 1841432
```

Hmm.. Also started at 1639354 and ran to final offset..

## ruby-kafka

```
gem install whirly
ruby ruby-kafka-v1.rb Bib
```

Result:

```
😮  consumed: 202079     min: 1639354    max: 1841432
```

Same... This specifies `start_from_beginning: true`, so should seek out the lowest offset, yet it's starting from 1639354 like the node versions... 
