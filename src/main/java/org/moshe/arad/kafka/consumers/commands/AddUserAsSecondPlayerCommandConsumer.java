package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.AddUserAsSecondPlayerCommand;
import org.moshe.arad.kafka.commands.AddUserAsWatcherCommand;
import org.moshe.arad.kafka.events.UserAddedAsSecondPlayerEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
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
public class AddUserAsSecondPlayerCommandConsumer extends SimpleCommandsConsumer{

	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private Logger logger = LoggerFactory.getLogger(AddUserAsSecondPlayerCommandConsumer.class);
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		AddUserAsSecondPlayerCommand addUserAsSecondPlayerCommand = convertJsonBlobIntoEvent(record.value()); 
		
		GameRoom gameRoomToAddSecondPlayer = lobbyRepository.getGameRoomToAddWatcherToOrAddSecondPlayer(addUserAsSecondPlayerCommand.getGameRoomName(), addUserAsSecondPlayerCommand.getUsername());
		
		if(gameRoomToAddSecondPlayer != null && gameRoomToAddSecondPlayer.getSecondPlayer().equals("")){												
			logger.info("Validation passed...");
			logger.info("Will add user as second player...");
			
			gameRoomToAddSecondPlayer.setSecondPlayer(addUserAsSecondPlayerCommand.getUsername());
			
			UserAddedAsSecondPlayerEvent userAddedAsSecondPlayerEvent = context.getBean(UserAddedAsSecondPlayerEvent.class);
			
			userAddedAsSecondPlayerEvent.setGameRoom(gameRoomToAddSecondPlayer);
			userAddedAsSecondPlayerEvent.setUuid(addUserAsSecondPlayerCommand.getUuid());
			userAddedAsSecondPlayerEvent.setArrived(new Date());
			userAddedAsSecondPlayerEvent.setClazz("UserAddedAsSecondPlayerEvent");
			userAddedAsSecondPlayerEvent.setUsername(addUserAsSecondPlayerCommand.getUsername());
			
			logger.info("Sending user Added As Second Player Event to kafka broker...");
			consumerToProducerQueue.getEventsQueue().put(userAddedAsSecondPlayerEvent);
			logger.info("event passed...");
		}
	}
	
	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

	private AddUserAsSecondPlayerCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, AddUserAsSecondPlayerCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}
