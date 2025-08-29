const { Kafka }= require("kafkajs");

exports.kafka= new Kafka({
    clientId:"my-app",
    brokers:["192.168.0.105:9092"]
})
