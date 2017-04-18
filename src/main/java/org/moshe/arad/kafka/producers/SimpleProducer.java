package org.moshe.arad.kafka.producers;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SimpleProducer implements Runnable{

	private final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
	 
	private Properties properties;
	
	@Autowired
	private ConsumerToProducerQueue consumerToProducerQueue;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	private boolean isRunning = true;
	private static final int PRODUCERS_NUM = 3;
	
	public SimpleProducer() {
		properties = new Properties();
		properties.put("bootstrap.servers", KafkaUtils.SERVERS);
		properties.put("key.serializer", KafkaUtils.KEY_STRING_SERIALIZER);
		properties.put("value.serializer", KafkaUtils.NEW_USER_JOINED_LOBBY_EVENT_SERIALIZER);
	}
	
	public SimpleProducer(String customValueSerializer) {
		properties = new Properties();
		properties.put("bootstrap.servers", KafkaUtils.SERVERS);
		properties.put("key.serializer", KafkaUtils.KEY_STRING_SERIALIZER);
		properties.put("value.serializer", customValueSerializer);
	}	
	
	private void takeMessagesFromConsumersAndPass(int numJobs){
		while(scheduledExecutor.getQueue().size() < numJobs){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if(scheduledExecutor.getActiveCount() == numJobs) continue;
			
			logger.info("Threads in pool's queue before schedule = " + scheduledExecutor.getQueue().size());
			scheduledExecutor.scheduleAtFixedRate(() -> {
				while(isRunning){
					try {
						NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = (NewUserJoinedLobbyEvent) consumerToProducerQueue.getEventsQueue().take();
						sendKafkaMessage(KafkaUtils.NEW_USER_JOINED_LOBBY_EVENT_TOPIC, newUserJoinedLobbyEvent);
					} catch (InterruptedException e) {
						logger.error("Failed to grab new user created event from queue.");
						e.printStackTrace();
					}
				}
			}, 0, 500, TimeUnit.MILLISECONDS);
			logger.info("Threads in pool's queue after schedule = " + scheduledExecutor.getQueue().size());
		}
	}

	private void sendKafkaMessage(String topicName, Object value){
		logger.info("Creating kafka producer.");
		Producer<String, Object> producer = new KafkaProducer<>(properties);
		logger.info("Kafka producer created.");
		
		logger.info("Sending message to topic = " + topicName + ", message = " + value.toString() + ".");
		ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topicName, value);
		producer.send(record);
		logger.info("Message sent.");
		producer.close();
		logger.info("Kafka producer closed.");
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
}
