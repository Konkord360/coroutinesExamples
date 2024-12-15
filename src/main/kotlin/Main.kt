@file:OptIn(ExperimentalCoroutinesApi::class)

package org.example

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.time.LocalDateTime
import java.util.*


fun main() = runBlocking {

    launch(Dispatchers.Default) {
        writeMessagesToTopic(setUpProducer())
    }


//    val rec = addTimestamp(readMessagesFromTopic(setUpClient()))
//    while(true) {
//        println(rec.receive())
//    }

    val readerChannel = Channel<String>()
    launch(Dispatchers.Default) {
       addTimestamp(readMessagesFromTopic(setUpClient()), readerChannel)
    }
    for(message in readerChannel) {
        println(message)
    }
    coroutineContext.cancelChildren()
}

private fun CoroutineScope.readMessagesFromTopic(consumer: KafkaConsumer<String, String>) = produce<String> {
    consumer.use {
        consumer.subscribe(listOf("test-topic"))
        consumer.seekToBeginning(consumer.assignment())
        while (true) {
            val records = consumer.poll(Duration.ofMillis(1000))
            if (records.count() > 0) {
                for (record in records) {
                    send(record.value())
                }
            }
        }
    }
}

private suspend fun CoroutineScope.readMessagesFromTopic(consumer: KafkaConsumer<String, String>, channel: Channel<String>) {
    consumer.use {
        consumer.subscribe(listOf("test-topic"))
        consumer.seekToBeginning(consumer.assignment())
        while (true) {
            val records = consumer.poll(Duration.ofMillis(1000))
            if (records.count() > 0) {
                for (record in records) {
                    channel.send(record.value())
                }
            }
        }
    }
}

private fun CoroutineScope.addTimestamp(messages: ReceiveChannel<String>): ReceiveChannel<String> = produce {
    for (message in messages) send("${LocalDateTime.now()}: $message")
}

private suspend fun CoroutineScope.addTimestamp(messages: ReceiveChannel<String>, senderChannel: Channel<String>) {
    for (message in messages) senderChannel.send("${LocalDateTime.now()}: $message")
}

private suspend fun writeMessagesToTopic(producer: KafkaProducer<String, String>) {
    var i = 0
    producer.use {
        while (true) {
            delay(1000)
            val record = ProducerRecord<String, String>("test-topic", "test$i")
            it.send(record)
            i++
        }
    }
}

private fun setUpProducer(): KafkaProducer<String, String> {
    val props = Properties()

    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = listOf("localhost:9092")
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

    return KafkaProducer<String, String>(props)
}

private fun setUpClient(): KafkaConsumer<String, String> {
    val consumerProperties = Properties()

    consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    consumerProperties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    consumerProperties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
    consumerProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    return KafkaConsumer<String, String>(consumerProperties)
}