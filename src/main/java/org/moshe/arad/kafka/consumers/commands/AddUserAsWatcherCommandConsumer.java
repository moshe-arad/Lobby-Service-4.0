package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.AddUserAsWatcherCommand;
import org.moshe.arad.kafka.commands.CloseGameRoomCommand;
import org.moshe.arad.kafka.commands.OpenNewGameRoomCommand;
import org.moshe.arad.kafka.events.CloseGameRoomEventAck;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEventAck;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEventAck;
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
public class AddUserAsWatcherCommandConsumer extends SimpleCommandsConsumer{

	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private ConsumerToProducerQueue consumerToProducerAckQueue;
	
	private Logger logger = LoggerFactory.getLogger(AddUserAsWatcherCommandConsumer.class);
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		AddUserAsWatcherCommand addUserAsWatcherCommand = convertJsonBlobIntoEvent(record.value()); 
		UserAddedAsWatcherEventAck userAddedAsWatcherEventAck = context.getBean(UserAddedAsWatcherEventAck.class);
		
		GameRoom gameRoomToAddWatcherTo = lobbyRepository.getGameRoomToAddWatcherTo(addUserAsWatcherCommand.getGameRoomName(), addUserAsWatcherCommand.getUsername());
		
		if(gameRoomToAddWatcherTo != null){												
			logger.info("Validation passed...");
			logger.info("Will add user as watcher...");
			
			gameRoomToAddWatcherTo.getWatchers().add(addUserAsWatcherCommand.getUsername());
			
			UserAddedAsWatcherEvent userAddedAsWatcherEvent = context.getBean(UserAddedAsWatcherEvent.class);
			
			userAddedAsWatcherEventAck.setUuid(addUserAsWatcherCommand.getUuid());
			userAddedAsWatcherEventAck.setArrived(new Date());
			userAddedAsWatcherEventAck.setGameRoom(gameRoomToAddWatcherTo);			
			userAddedAsWatcherEventAck.setUserAddedAsWatcher(true);
			userAddedAsWatcherEventAck.setUsername(addUserAsWatcherCommand.getUsername());
			
			userAddedAsWatcherEvent.setGameRoom(gameRoomToAddWatcherTo);
			userAddedAsWatcherEvent.setUuid(addUserAsWatcherCommand.getUuid());
			userAddedAsWatcherEvent.setArrived(new Date());
			userAddedAsWatcherEvent.setClazz("UserAddedAsWatcherEvent");
			userAddedAsWatcherEvent.setUsername(addUserAsWatcherCommand.getUsername());
			
			logger.info("Will reply with success ack event...");
			consumerToProducerAckQueue.getEventsQueue().put(userAddedAsWatcherEventAck);
			logger.info("Ack event passed...");
			
			logger.info("Sending close game room event to kafka broker...");
			consumerToProducerQueue.getEventsQueue().put(userAddedAsWatcherEvent);
			logger.info("event passed...");
		}
		else{			
			userAddedAsWatcherEventAck.setUuid(addUserAsWatcherCommand.getUuid());
			userAddedAsWatcherEventAck.setArrived(new Date());
			userAddedAsWatcherEventAck.setGameRoom(gameRoomToAddWatcherTo);			
			userAddedAsWatcherEventAck.setUserAddedAsWatcher(false);
			userAddedAsWatcherEventAck.setUsername(addUserAsWatcherCommand.getUsername());
			
			logger.info("Will reply with failure ack event...");
			consumerToProducerAckQueue.getEventsQueue().put(userAddedAsWatcherEventAck);
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

	private AddUserAsWatcherCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, AddUserAsWatcherCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}
