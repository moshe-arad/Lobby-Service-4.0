package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.CloseGameRoomCommand;
import org.moshe.arad.kafka.commands.OpenNewGameRoomCommand;
import org.moshe.arad.kafka.events.CloseGameRoomEventAck;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEventAck;
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
public class CloseGameRoomCommandConsumer extends SimpleCommandsConsumer{

	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private ConsumerToProducerQueue consumerToProducerAckQueue;
	
	private Logger logger = LoggerFactory.getLogger(CloseGameRoomCommandConsumer.class);
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		CloseGameRoomCommand closeGameRoomCommand = convertJsonBlobIntoEvent(record.value()); 
		CloseGameRoomEventAck closeGameRoomEventAck = context.getBean(CloseGameRoomEventAck.class);
		GameRoom GameRoomToClose = lobbyRepository.getGameRoomToClose(closeGameRoomCommand.getOpenedBy());
		
		if(GameRoomToClose != null){												
			logger.info("Validation passed...");
			logger.info("Will close game Room...");
			
			GameRoomClosedEvent gameRoomClosedEvent = context.getBean(GameRoomClosedEvent.class);
			
			closeGameRoomEventAck.setUuid(closeGameRoomCommand.getUuid());
			closeGameRoomEventAck.setArrived(new Date());
			closeGameRoomEventAck.setGameRoom(GameRoomToClose);			
			closeGameRoomEventAck.setGameRoomClosed(true);
			
			gameRoomClosedEvent.setGameRoom(GameRoomToClose);
			gameRoomClosedEvent.setUuid(closeGameRoomCommand.getUuid());
			gameRoomClosedEvent.setArrived(new Date());
			gameRoomClosedEvent.setClazz("GameRoomClosedEvent");
			
			logger.info("Will reply with success ack event...");
			consumerToProducerAckQueue.getEventsQueue().put(closeGameRoomEventAck);
			logger.info("Ack event passed...");
			
			logger.info("Sending close game room event to kafka broker...");
			consumerToProducerQueue.getEventsQueue().put(gameRoomClosedEvent);
			logger.info("event passed...");
		}
		else{			
			closeGameRoomEventAck.setUuid(closeGameRoomCommand.getUuid());
			closeGameRoomEventAck.setGameRoom(GameRoomToClose);			
			closeGameRoomEventAck.setGameRoomClosed(false);
			
			logger.info("Will reply with failure ack event...");
			consumerToProducerAckQueue.getEventsQueue().put(closeGameRoomEventAck);
			logger.info("Ack event passed...");
		}
		
		
	}
	
	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

	public void setConsumerToProducerAckQueue(ConsumerToProducerQueue consumerToProducerAckQueue) {
		this.consumerToProducerAckQueue = consumerToProducerAckQueue;
	}

	private CloseGameRoomCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, CloseGameRoomCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}
