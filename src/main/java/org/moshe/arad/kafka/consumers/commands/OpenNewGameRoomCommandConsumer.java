package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.OpenNewGameRoomCommand;
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
public class OpenNewGameRoomCommandConsumer extends SimpleCommandsConsumer{

	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private Logger logger = LoggerFactory.getLogger(OpenNewGameRoomCommandConsumer.class);
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		OpenNewGameRoomCommand openNewGameRoomCommand = convertJsonBlobIntoEvent(record.value()); 
		
		if(!lobbyRepository.isUserEngagedInOtherRoom(openNewGameRoomCommand.getUsername())){												
			logger.info("Validation passed...");
			logger.info("Will send new game Room opened event...");
			
			NewGameRoomOpenedEvent newGameRoomOpenedEvent = context.getBean(NewGameRoomOpenedEvent.class);
			GameRoom gameRoom = context.getBean(GameRoom.class);
		
			gameRoom.setName("A" + openNewGameRoomCommand.getUuid().toString().replaceAll("-", "").substring(0, 14));
			gameRoom.setOpenBy(openNewGameRoomCommand.getUsername());
			gameRoom.setSecondPlayer("");
			gameRoom.setWatchers(new ArrayList<String>());
			
			newGameRoomOpenedEvent.setGameRoom(gameRoom);
			newGameRoomOpenedEvent.setUuid(openNewGameRoomCommand.getUuid());
			newGameRoomOpenedEvent.setArrived(new Date());
			newGameRoomOpenedEvent.setClazz("NewGameRoomOpenedEvent");
			
			logger.info("Sending new game room opened event to kafka broker...");
			consumerToProducerQueue.getEventsQueue().put(newGameRoomOpenedEvent);
			logger.info("event passed...");
		}
	}
	
	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

	private OpenNewGameRoomCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, OpenNewGameRoomCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}
