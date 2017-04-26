package org.moshe.arad.kafka.producers.events;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.producers.ISimpleProducer;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * 
 * @author moshe-arad
 *
 * @param <T> is the event that we want to pass
 * 
 * important to set topic and properties before usage
 */
@Component
@Scope("prototype")
public class SimpleEventsProducer <T extends BackgammonEvent> implements ISimpleProducer<T>, Runnable {

	private final Logger logger = LoggerFactory.getLogger(SimpleEventsProducer.class);
	
	private SimpleProducerConfig simpleProducerConfig;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	private boolean isRunning = true;
	private static final int PRODUCERS_NUM = 3;
	private String topic;
	
	public SimpleEventsProducer() {
	}
	
	@Override
    public void sendKafkaMessage(T event){
		try{
			logger.info("Front Service is about to send a Command to topic=" + topic + ", Event=" + event);
			sendMessage(event);
			logger.info("Message sent successfully, Front Service sent a Command to topic=" + topic + ", Event=" + event);
		}
		catch(Exception ex){
			logger.error("Failed to sent message, Front Service failed to send a Command to topic=" + topic + ", Event=" + event);
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	private void sendMessage(T event){
		logger.info("Creating kafka producer.");
		Producer<String, String> producer = new KafkaProducer<>(simpleProducerConfig.getProperties());
		logger.info("Kafka producer created.");
		
		logger.info("Sending message to topic = " + topic + ", message = " + event.toString() + ".");
		String eventJsonBlob = convertEventIntoJsonBlob(event);
		logger.info("Sending message to topic = " + topic + ", JSON message = " + eventJsonBlob + ".");
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, eventJsonBlob);
		producer.send(record);
		logger.info("Message sent.");
		producer.close();
		logger.info("Kafka producer closed.");
	}

	private String convertEventIntoJsonBlob(T event){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(event);
		} catch (JsonProcessingException e) {
			logger.error("Failed to convert event into JSON blob...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private void takeMessagesFromConsumersAndPass(int numJobs){
		while(scheduledExecutor.getQueue().size() < numJobs){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			if(scheduledExecutor.getActiveCount() == numJobs) continue;
			
			scheduledExecutor.scheduleAtFixedRate(() -> {
				while(isRunning){
					try {						
						T backgammonEvent = (T) consumerToProducerQueue.getEventsQueue().take();
						sendKafkaMessage(backgammonEvent);
					} catch (InterruptedException e) {
						logger.error("Failed to grab new user created event from queue.");
						e.printStackTrace();
					}
				}
			}, 0, 500, TimeUnit.MILLISECONDS);
		}
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

	@Override
	public void run() {
		this.takeMessagesFromConsumersAndPass(PRODUCERS_NUM);		
	}

	public SimpleProducerConfig getSimpleProducerConfig() {
		return simpleProducerConfig;
	}

	public void setSimpleProducerConfig(SimpleProducerConfig simpleProducerConfig) {
		this.simpleProducerConfig = simpleProducerConfig;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}	
}
