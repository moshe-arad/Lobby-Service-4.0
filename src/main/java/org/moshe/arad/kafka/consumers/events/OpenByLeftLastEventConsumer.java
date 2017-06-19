package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.OpenByLeftLastEvent;
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
public class OpenByLeftLastEventConsumer extends SimpleEventsConsumer {
	
	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	Logger logger = LoggerFactory.getLogger(OpenByLeftLastEventConsumer.class);
	
	public OpenByLeftLastEventConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
    	try{
    		OpenByLeftLastEvent openByLeftLastEvent = convertJsonBlobIntoEvent(record.value());    		
    		logger.info("Logged Out OpenBy Left Before Game Started Event recieved, " + record.value());
    		
    		GameRoomClosedEvent gameRoomClosedEvent = context.getBean(GameRoomClosedEvent.class);
    		gameRoomClosedEvent.setUuid(openByLeftLastEvent.getUuid());
    		gameRoomClosedEvent.setArrived(new Date());
    		gameRoomClosedEvent.setClazz("GameRoomClosedEvent");
    		gameRoomClosedEvent.setLoggedOutUserName(openByLeftLastEvent.getOpenBy());
    		gameRoomClosedEvent.setGameRoom(openByLeftLastEvent.getGameRoom());
    		
    		consumerToProducerQueue.getEventsQueue().put(gameRoomClosedEvent);
    	}
		catch (Exception ex) {
			logger.error("Error occured Logged Out Event Consumer...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	private OpenByLeftLastEvent convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, OpenByLeftLastEvent.class);
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
