package com.tamajit.producer;

import org.apache.htrace.shaded.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
public class KafkaWriter {
	 String topic;
	    String sync;
	    private Properties kafkaProps = new Properties();
	    private Producer<String, String> producer;

	    public KafkaWriter(String topic) {
	        this.topic = topic;
	    }

	    public void configure(String brokerList, String sync) {
	        this.sync = sync;
	        kafkaProps.put("bootstrap.servers", brokerList);

	        // This is mandatory, even though we don't send keys
	        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        kafkaProps.put("acks", "1");

	        // how many times to retry when produce request fails?
	        kafkaProps.put("retries", "3");
	        kafkaProps.put("linger.ms", 5);
	    }

	    public void start() {
	        producer = new KafkaProducer<String, String>(kafkaProps);
	    }

	    public void produce(String value) throws ExecutionException, InterruptedException {
	        if (sync.equals("sync"))
	            produceSync(value);
	        else if (sync.equals("async"))
	            produceAsync(value);
	        else throw new IllegalArgumentException("Expected sync or async, got " + sync);

	    }

	    public void close() {
	        producer.close();
	    }

	    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
	    private void produceSync(String value) throws ExecutionException, InterruptedException {
	        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
	        producer.send(record).get();

	    }

	    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
	    private void produceAsync(String value) {
	        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
	        producer.send(record, new DemoProducerCallback());
	    }

	    private class DemoProducerCallback implements Callback {

	        @Override
	        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	            if (e != null) {
	                System.out.println("Error producing to topic " + recordMetadata.topic());
	                e.printStackTrace();
	            }
	        }
	    }
	    
	    public static void main(String[] args) throws ExecutionException, InterruptedException, JsonProcessingException {
	       
	    	String brokerList = "10.241.17.172:9092";
	        String topic = "topic1";
	    	String sync="sync";//aync or async
	        KafkaWriter kafkaWriter = new KafkaWriter(topic);
	        kafkaWriter.configure(brokerList, sync);
	        kafkaWriter.start();
	        
//	        EC2
	        
//	        long startTime = System.currentTimeMillis();
	        System.out.println("Starting...");
	        StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();
	        ObjectMapper om=new ObjectMapper();
	        while(true) {
	            StockTrade trade = stockTradeGenerator.getRandomTrade();
	            kafkaWriter.produce(om.writeValueAsString(trade));
	            Thread.sleep(1000);
	        }

		}
}
