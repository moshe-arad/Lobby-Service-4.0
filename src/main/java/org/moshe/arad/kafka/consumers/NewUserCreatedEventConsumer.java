package org.moshe.arad.kafka.consumers;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.moshe.arad.Lobby;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.EventFactory;
import org.moshe.arad.kafka.events.Events;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NewUserCreatedEventConsumer implements Runnable {
	
	Logger logger = LoggerFactory.getLogger(NewUserCreatedEventConsumer.class);
	private static final int CONSUMERS_NUM = 3;
	private Properties properties;
	
	private Consumer<String, NewUserCreatedEvent> consumer;
	private boolean isRunning = true;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	
	@Autowired
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	@Autowired
	private Lobby lobby;
	
	public NewUserCreatedEventConsumer() {
		properties = new Properties();
		properties.put("bootstrap.servers", KafkaUtils.SERVERS);
		properties.put("key.deserializer", KafkaUtils.KEY_STRING_DESERIALIZER);
		properties.put("value.deserializer", KafkaUtils.NEW_USER_CREATED_EVENT_DESERIALIZER);
		properties.put("group.id", KafkaUtils.NEW_USER_CREATED_EVENT_GROUP);
		consumer = new KafkaConsumer<>(properties);
	}
	
	public NewUserCreatedEventConsumer(String customValueDeserializer, String groupName, Map<String,BackgammonUser> users, String topicName) {
		properties = new Properties();
		properties.put("bootstrap.servers", KafkaUtils.SERVERS);
		properties.put("group.id", groupName);
		properties.put("key.deserializer", KafkaUtils.NEW_USER_CREATED_EVENT_DESERIALIZER);
		properties.put("value.deserializer", customValueDeserializer);
		consumer = new KafkaConsumer<>(properties);
	}

	private void executeConsumers(int numConsumers, String topicName){
		
		while(scheduledExecutor.getQueue().size() < numConsumers){
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if(scheduledExecutor.getActiveCount() == numConsumers) continue;
			
//			logger.info("Threads in pool's queue before schedule = " + scheduledExecutor.getQueue().size());
			scheduledExecutor.scheduleAtFixedRate( () -> {
				consumer.subscribe(Arrays.asList(topicName));
	    		
	    		while (isRunning){
	                ConsumerRecords<String, NewUserCreatedEvent> records = consumer.poll(100);
	                for (ConsumerRecord<String, NewUserCreatedEvent> record : records){
	                	logger.info("New User Created Event record recieved, " + record.value().getBackgammonUser());	             	                		               
	                	NewUserCreatedEvent newUserCreatedEvent = (NewUserCreatedEvent)record.value();
	                	logger.info("adding new user to lobby...");
	                	lobby.addBackgammonUserToLobby(newUserCreatedEvent.getBackgammonUser());
	                	logger.info("User added to lobby...");
	                	logger.info("creating new user joined lobby event...");
	                	NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = (NewUserJoinedLobbyEvent) EventFactory.getEvent(Events.NewUserJoinedLobbyEvent, newUserCreatedEvent.getBackgammonUser(), null);
	                	logger.info("passing new user joined lobby event to producer...");
	                	consumerToProducerQueue.getEventsQueue().put(newUserJoinedLobbyEvent);
	                }	              	             
	    		}
		        consumer.close();
		        
			} , 0, 100, TimeUnit.MILLISECONDS);
//			logger.info("Threads in pool's queue after schedule = " + scheduledExecutor.getQueue().size());
		}
	}
	
	@Override
	public void run() {
		this.executeConsumers(CONSUMERS_NUM, KafkaUtils.NEW_USER_CREATED_EVENT_TOPIC);
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
}




	