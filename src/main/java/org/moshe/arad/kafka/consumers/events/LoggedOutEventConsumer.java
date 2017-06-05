package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.entities.Status;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.LoggedInEvent;
import org.moshe.arad.kafka.events.LoggedOutEvent;
import org.moshe.arad.kafka.events.LoggedOutUserLeftLobbyEvent;
import org.moshe.arad.repository.LobbyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class LoggedOutEventConsumer extends SimpleEventsConsumer {
	
	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	Logger logger = LoggerFactory.getLogger(LoggedOutEventConsumer.class);
	
	private Map<Class<? extends BackgammonEvent>,ConsumerToProducerQueue> consumerToProducer;
	
	public LoggedOutEventConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
    	try{
    		LoggedOutEvent loggedOutEvent = convertJsonBlobIntoEvent(record.value());    		
    		logger.info("Logged Out Event recieved, " + record.value());
    		BackgammonUser user = loggedOutEvent.getBackgammonUser();
    		List<GameRoom> rooms = lobbyRepository.getUserEngagedGameRooms(user.getUserName());
    		
    		if(rooms.size() == 0){
    			logger.info("User is not engaged in any room...");
    			logger.info("User is leaving lobby...");
    			LoggedOutUserLeftLobbyEvent loggedOutUserLeftLobbyEvent = context.getBean(LoggedOutUserLeftLobbyEvent.class);
    			loggedOutUserLeftLobbyEvent.setUuid(loggedOutEvent.getUuid());
    			loggedOutUserLeftLobbyEvent.setClazz("LoggedOutUserLeftLobbyEvent");
    			loggedOutUserLeftLobbyEvent.setArrived(new Date());
    			loggedOutUserLeftLobbyEvent.setBackgammonUser(loggedOutEvent.getBackgammonUser());
    			
    			consumerToProducer.get(LoggedOutUserLeftLobbyEvent.class).getEventsQueue().put(loggedOutUserLeftLobbyEvent);
    		}
    	}
		catch (Exception ex) {
			logger.error("Error occured Logged Out Event Consumer...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	private LoggedOutEvent convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, LoggedOutEvent.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {

	}

	public Map<Class<? extends BackgammonEvent>, ConsumerToProducerQueue> getConsumerToProducer() {
		return consumerToProducer;
	}

	public void setConsumerToProducer(Map<Class<? extends BackgammonEvent>, ConsumerToProducerQueue> consumerToProducer) {
		this.consumerToProducer = consumerToProducer;
	}
}