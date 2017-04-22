package org.moshe.arad.kafka.consumers.events;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author moshe-arad
 *
 * @param <T> is the event to consume
 * 
 * important to set properties and topic before usage
 */
public abstract class SimpleBackgammonEventsConsumer <T extends BackgammonEvent> implements Runnable {

	Logger logger = LoggerFactory.getLogger(SimpleBackgammonEventsConsumer.class);
	private static final int CONSUMERS_NUM = 3;
	
	private Consumer<String, T> consumer;
	private boolean isRunning = true;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	private String topic;
	private SimpleConsumerConfig simpleConsumerConfig;
	
	public SimpleBackgammonEventsConsumer() {
	}
	
	public SimpleBackgammonEventsConsumer(SimpleConsumerConfig simpleConsumerConfig, String topic) {
		this.simpleConsumerConfig = simpleConsumerConfig;
		consumer = new KafkaConsumer<String,T>(simpleConsumerConfig.getProperties());
		this.topic = topic;
	}

	private void executeConsumers(int numConsumers){
		
		while(scheduledExecutor.getQueue().size() < numConsumers){
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if(scheduledExecutor.getActiveCount() == numConsumers) continue;
			
			scheduledExecutor.scheduleAtFixedRate( () -> {
				consumer.subscribe(Arrays.asList(topic));
	    		
	    		while (isRunning){
	                ConsumerRecords<String, T> records = consumer.poll(100);
	                for (ConsumerRecord<String, T> record : records){
	                	consumerOperations(record);	                	
	                }	              	             
	    		}
		        consumer.close();
		        
			} , 0, 100, TimeUnit.MILLISECONDS);
		}
	}
	
	public void initConsumer(){
		consumer = new KafkaConsumer<String,T>(simpleConsumerConfig.getProperties());
	}
	
	public abstract void consumerOperations(ConsumerRecord<String,T> record);
	
	@Override
	public void run() {
		this.executeConsumers(CONSUMERS_NUM);
	}

	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}
	
	public ScheduledThreadPoolExecutor getScheduledExecutor() {
		return scheduledExecutor;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public SimpleConsumerConfig getSimpleConsumerConfig() {
		return simpleConsumerConfig;
	}

	public void setSimpleConsumerConfig(SimpleConsumerConfig simpleConsumerConfig) {
		this.simpleConsumerConfig = simpleConsumerConfig;
	}
}
