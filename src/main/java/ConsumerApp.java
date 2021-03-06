import org.apache.kafka.common.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonArray;

import org.apache.kafka.clients.consumer.*;

import java.util.*;

public class ConsumerApp {
    public static void main(String[] args){

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("fetch.min.bytes", 1);
        props.put("group.id", "");
        props.put("heartbeat.interval.ms", 3000);
        props.put("max.partition.fetch.bytes", 1048576);
        props.put("session.timeout.ms", 30000);
        props.put("auto.offset.reset", "latest");
        props.put("connections.max.idle.ms", 540000);
        props.put("enable.auto.commit", true);
        props.put("exclude.internal.topics", true);
        props.put("max.poll.records", 2147483647);
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put("request.timeout.ms", 40000);
        props.put("auto.commit.interval.ms", 5000);
        props.put("fetch.max.wait.ms", 500);
        props.put("metadata.max.age.ms", 300000);
        props.put("reconnect.backoff.ms", 50);
        props.put("retry.backoff.ms", 100);
        props.put("client.id", "");


        // Create a KafkaConsumer instance and configure it with properties.
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        partitions.add(new TopicPartition("New_topic_demo", 1)); // Adds a TopicPartition instance representing a topic and a partition.
        myConsumer.assign(partitions);
        Set<TopicPartition> assignedPartitions = myConsumer.assignment();
        printSet(assignedPartitions);

        try {
            while (true){
                ConsumerRecords records = myConsumer.poll(1000);
                printRecords(records);
            }
        } finally {
            myConsumer.close();
        }

    }

    private static void printSet(Set<TopicPartition> collection){
        if (collection.isEmpty()) {
            System.out.println("I do not have any partitions assigned yet...");
        }
        else {
            System.out.println("I am assigned to following partitions:");
            for (TopicPartition partition: collection){
                System.out.println(String.format("Partition: %s in Topic: %s", Integer.toString(partition.partition()), partition.topic()));
            }
        }
    }

    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
