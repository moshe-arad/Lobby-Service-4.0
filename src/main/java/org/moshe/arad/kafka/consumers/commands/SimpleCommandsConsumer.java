package org.moshe.arad.kafka.consumers.commands;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.ICommand;
import org.moshe.arad.kafka.consumers.ISimpleConsumer;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * 
 * @author moshe-arad
 *
 * @param <T> is the event to consume
 * 
 * important to set properties and topic before usage
 */
@Component
@Scope("prototype")
public abstract class SimpleCommandsConsumer implements Runnable, ISimpleCommandConsumer {

	Logger logger = LoggerFactory.getLogger(SimpleCommandsConsumer.class);
	
	private Consumer<String, String> consumer;
	private boolean isRunning = true;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	private String topic;
	private SimpleConsumerConfig simpleConsumerConfig;
	
	public SimpleCommandsConsumer() {
	}

	private void executeConsumers(){
			scheduledExecutor.scheduleAtFixedRate( () -> {
				try{
					while (isRunning){
		                ConsumerRecords<String, String> records = consumer.poll(100);
		                for (ConsumerRecord<String, String> record : records){
		                	consumerOperations(record);	                	
		                }
		                consumer.commitAsync();
		    		}
				}
				catch (Exception e) {
					logger.error("Failed while consuming data...");
					logger.error(e.getMessage());
					e.printStackTrace();
				}
				finally {
					consumer.commitSync();
					consumer.close();
				}
	    				       		        
			} , 0, 100, TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void initConsumer(){
		consumer = new KafkaConsumer<String,String>(simpleConsumerConfig.getProperties());
		consumer.subscribe(Arrays.asList(topic));
	}
	
	public abstract void consumerOperations(ConsumerRecord<String,String> record);
	
	@Override
	public void run() {
		this.executeConsumers();
	}

	public boolean isRunning() {
		return isRunning;
	}

	@Override
	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}
	
	@Override
	public ScheduledThreadPoolExecutor getScheduledExecutor() {
		return scheduledExecutor;
	}

	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public SimpleConsumerConfig getSimpleConsumerConfig() {
		return simpleConsumerConfig;
	}

	@Override
	public void setSimpleConsumerConfig(SimpleConsumerConfig simpleConsumerConfig) {
		this.simpleConsumerConfig = simpleConsumerConfig;
	}
	
	@Override
	public void closeConsumer(){
		consumer.close();
	}
}
