package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.entities.Status;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.LoggedInEvent;
import org.moshe.arad.services.Lobby;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class LoggedInEventConsumer extends SimpleEventsConsumer {

	@Autowired
	private Lobby lobby;
	
	@Autowired
	private ApplicationContext context;
	
	Logger logger = LoggerFactory.getLogger(LoggedInEventConsumer.class);
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	public LoggedInEventConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
    	try{
    		LoggedInEvent loggedInEvent = convertJsonBlobIntoEvent(record.value());    		
    		logger.info("Logged In Event record recieved, " + record.value());	             
        	
    		ExistingUserJoinedLobbyEvent existingUserJoinedLobbyEvent = context.getBean(ExistingUserJoinedLobbyEvent.class);
    		existingUserJoinedLobbyEvent.setUuid(loggedInEvent.getUuid());
    		existingUserJoinedLobbyEvent.setArrived(new Date());
    		BackgammonUser user = loggedInEvent.getBackgammonUser();
    		user.setStatus(Status.InLobby);
    		existingUserJoinedLobbyEvent.setBackgammonUser(user);
    		existingUserJoinedLobbyEvent.setClazz("ExistingUserJoinedLobbyEvent");
    		
    		consumerToProducerQueue.getEventsQueue().put(existingUserJoinedLobbyEvent);
    		logger.info("Existing user joined lobby event was created and passed to producer...");
    		
        	
    	}
		catch (Exception ex) {
			logger.error("Error occured while creating an existing user joined lobby event...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	private LoggedInEvent convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, LoggedInEvent.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

}
