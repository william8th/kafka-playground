package com.williamheng.streams

import com.williamheng.avro.TestData
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.ProcessorSupplier
import java.util.Properties


fun main(args: Array<String>) {
    val streamsBuilder = StreamsBuilder()
    streamsBuilder
            .stream<String, SpecificRecordBase>("my-topic")
            .peek { key, value -> println("Processing $key") }
            .process(processorSupplier())

    val topology = streamsBuilder.build()

    val streams = KafkaStreams(topology, streamsProperties())

    println("Running streams application")
    streams.start()
}

fun processorSupplier() = ProcessorSupplier<String, SpecificRecordBase> { EchoProcessor() }

class EchoProcessor: Processor<String, SpecificRecordBase> {
    override fun init(processorContext: ProcessorContext?) {
        //
    }

    override fun process(key: String, value: SpecificRecordBase) {
        val testData = value as TestData
        println("Received ${testData.getId()}")
    }

    override fun close() {
        // Do nothing
    }
}

fun streamsProperties(): Properties {
    val properties = Properties()
    properties[StreamsConfig.APPLICATION_ID_CONFIG] = "sample-streams-app"
    properties[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    properties[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
    properties[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
    return properties
}
