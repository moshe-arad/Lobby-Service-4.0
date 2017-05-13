package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.Status;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEventAck;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.services.Lobby;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class NewUserCreatedEventAckConsumer extends SimpleEventsConsumer {

	@Autowired
	private Lobby lobby;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	Logger logger = LoggerFactory.getLogger(NewUserCreatedEventAckConsumer.class);
	
	public NewUserCreatedEventAckConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
		NewUserCreatedEventAck newUserCreatedEventAck = convertJsonBlobIntoEvent(record.value());
		logger.info("New User Created Event **Ack** record recieved, " + record.value());	             	                		               
    	logger.info("adding new user to lobby...");
    	lobby.addBackgammonUserToLobby(newUserCreatedEventAck.getBackgammonUser());
    	logger.info("User added to lobby...");
    	logger.info("creating new user joined lobby event...");
    	newUserCreatedEventAck.getBackgammonUser().setStatus(Status.InLobby);
    	NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = new NewUserJoinedLobbyEvent(newUserCreatedEventAck.getUuid(), 
    			2, 2, new Date(),"newUserJoinedLobbyEvent", newUserCreatedEventAck.getBackgammonUser());
    	logger.info("passing new user joined lobby event to producer...");
    	consumerToProducerQueue.getEventsQueue().put(newUserJoinedLobbyEvent);
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}
	
	private NewUserCreatedEventAck convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, NewUserCreatedEventAck.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}




	