package com.williamheng

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

fun main(args: Array<String>) {

    val properties = Properties()
    properties["bootstrap.servers"] = "localhost:9092"
    properties["group.id"] = "consumer-tutorial"
    properties["key.deserializer"] = StringDeserializer::class.java.canonicalName
    properties["value.deserializer"] = StringDeserializer::class.java.canonicalName

    val consumer = KafkaConsumer<String, String>(properties)

    consumer.subscribe(listOf("my-topic"))

    while (true) {
        try {
            val records = consumer.poll(Duration.ofSeconds(1L))
            // todo
        } finally {
            consumer.close()
        }
    }

}