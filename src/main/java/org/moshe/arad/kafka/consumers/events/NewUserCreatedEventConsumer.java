package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.Location;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.services.Lobby;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class NewUserCreatedEventConsumer extends SimpleEventsConsumer {

	@Autowired
	private Lobby lobby;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	Logger logger = LoggerFactory.getLogger(NewUserCreatedEventConsumer.class);
	
	public NewUserCreatedEventConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
		NewUserCreatedEvent newUserCreatedEvent = convertJsonBlobIntoEvent(record.value());
		logger.info("New User Created Event record recieved, " + record.value());	             	                		               
    	logger.info("adding new user to lobby...");
    	lobby.addBackgammonUserToLobby(newUserCreatedEvent.getBackgammonUser());
    	logger.info("User added to lobby...");
    	logger.info("creating new user joined lobby event...");
    	newUserCreatedEvent.getBackgammonUser().setLocation(Location.Lobby);
    	NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = new NewUserJoinedLobbyEvent(newUserCreatedEvent.getUuid(), 
    			2, 2, new Date(),"newUserJoinedLobbyEvent", newUserCreatedEvent.getBackgammonUser());
    	logger.info("passing new user joined lobby event to producer...");
    	consumerToProducerQueue.getEventsQueue().put(newUserJoinedLobbyEvent);
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}
	
	private NewUserCreatedEvent convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, NewUserCreatedEvent.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}




	