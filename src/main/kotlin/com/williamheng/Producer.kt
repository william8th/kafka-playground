package com.williamheng

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import kotlin.reflect.KClass

class Producer

fun main(args: Array<String>) {

    val log = LoggerFactory.getLogger(Producer::class.java)

    val properties = producerProperties("dumb-producer", StringSerializer::class, StringSerializer::class)
    val producer = KafkaProducer<String, String>(properties)

    log.info("Trying to produce...")
    for (i in 1..10) {
        log.info("Sending $i...")
        producer.send(ProducerRecord("my-topic", "$i-key", "$i-value"))
    }

    producer.close()
}

fun producerProperties(clientId: String, keySerializerClass: KClass<out Any>, valueSerializerClass: KClass<out Any>): Properties {
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    properties[ProducerConfig.CLIENT_ID_CONFIG] = clientId
    properties[ProducerConfig.ACKS_CONFIG] = "all"
    properties[ProducerConfig.RETRIES_CONFIG] = 100
    properties[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 30000
    properties[ProducerConfig.BATCH_SIZE_CONFIG] = 16384
    properties[ProducerConfig.LINGER_MS_CONFIG] = 1
    properties[ProducerConfig.BUFFER_MEMORY_CONFIG] = 33554432
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializerClass.java.canonicalName
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializerClass.java.canonicalName
    return properties
}
