# kafkadebug

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
consumed: 404150	min: 1639354	max: 1841430
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
