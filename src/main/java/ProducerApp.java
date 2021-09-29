import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;

import com.google.gson.JsonArray;

import java.text.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ProducerApp {

    public static void main(String[] args) throws InterruptedException, ExecutionException{

    	// Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "none");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("client.id", "");
        props.put("linger.ms", 0);
        props.put("max.block.ms", 60000);
        props.put("max.request.size", 1048576);
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put("request.timeout.ms", 30000);
        props.put("timeout.ms", 30000);
        props.put("max.in.flight.requests.per.connection", 5);
        props.put("retry.backoff.ms", 5);

               
        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        Properties adminprops=new Properties();
        adminprops.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
       
       AdminClient admin=AdminClient.create(adminprops);
       CreateTopicsResult newTopic=admin.createTopics(Collections.singletonList(new NewTopic("New_topic_demo",2,(short) 1)));
       System.out.println(newTopic.values());
       
       ProducerRecord <String, String>producerRecord=new ProducerRecord<String, String>("New_topic_demo", "How are you");
       myProducer.send(producerRecord).get();
    }
}
