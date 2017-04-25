package org.moshe.arad.kafka.consumers.events;

import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.Lobby;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NewUserCreatedEventConsumer extends SimpleBackgammonEventsConsumer<NewUserCreatedEvent> {

	@Autowired
	private Lobby lobby;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	Logger logger = LoggerFactory.getLogger(NewUserCreatedEventConsumer.class);
	
	public NewUserCreatedEventConsumer() {
	}
	
	public NewUserCreatedEventConsumer(SimpleConsumerConfig simpleConsumerConfig, String topic) {
		super(simpleConsumerConfig, topic);
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,NewUserCreatedEvent> record) {
		logger.info("New User Created Event record recieved, " + record.value());	             	                		               
    	NewUserCreatedEvent newUserCreatedEvent = (NewUserCreatedEvent)record.value();
    	logger.info("adding new user to lobby...");
    	lobby.addBackgammonUserToLobby(newUserCreatedEvent.getBackgammonUser());
    	logger.info("User added to lobby...");
    	logger.info("creating new user joined lobby event...");
    	NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = new NewUserJoinedLobbyEvent(record.value().getUuid(), 
    			2, 2, new Date(), record.value().getBackgammonUser());
    	logger.info("passing new user joined lobby event to producer...");
    	consumerToProducerQueue.getEventsQueue().put(newUserJoinedLobbyEvent);
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}		
}




	