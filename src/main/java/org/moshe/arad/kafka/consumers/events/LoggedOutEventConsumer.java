package org.moshe.arad.kafka.consumers.events;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.entities.Status;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.LoggedOutEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.WatcherRemovedEvent;
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
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private ConsumerToProducerQueue consumerToProducerWatcherQueue;
	
	private ConsumerToProducerQueue consumerToProducerOpenedByQueue;
	
	@Autowired
	private ApplicationContext context;
	
	Logger logger = LoggerFactory.getLogger(LoggedOutEventConsumer.class);
	
	public LoggedOutEventConsumer() {
	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
		LoggedOutEvent loggedOutEvent = convertJsonBlobIntoEvent(record.value());
		logger.info("Logged Out Event record recieved, " + record.value());	             	                		               
    	logger.info("checking whether user is engaged in game rooms...");
    	
    	List<GameRoom> gameRooms = lobbyRepository.getUserEngagedGameRooms(loggedOutEvent.getBackgammonUser().getUserName());
    	
    	if(gameRooms.size() > 1) throw new RuntimeException("User is engaged in more than one room");
    	else if(gameRooms.size() == 1){
    		logger.info("User is engaged in one room...");
    		//TODO handle 2nd player
    		boolean isOpenedBy = (gameRooms.get(0).getOpenBy().equals(loggedOutEvent.getBackgammonUser().getUserName())) ? true : false;
    		boolean isWatcher = (gameRooms.get(0).getWatchers().contains(loggedOutEvent.getBackgammonUser().getUserName())) ? true : false;
    		
    		if(isOpenedBy){
    			GameRoomClosedEvent gameRoomClosedEvent = context.getBean(GameRoomClosedEvent.class);
    			gameRoomClosedEvent.setUuid(loggedOutEvent.getUuid());
    			gameRoomClosedEvent.setArrived(new Date());
    			gameRoomClosedEvent.setClazz("GameRoomClosedEvent");
    			gameRoomClosedEvent.setClosedByUserName(loggedOutEvent.getBackgammonUser().getUserName());
    			gameRoomClosedEvent.setGameRoom(gameRooms.get(0));
    			
    			logger.info("passing game Room Closed Event to producer...");
    			consumerToProducerOpenedByQueue.getEventsQueue().put(gameRoomClosedEvent);
    		}
    		else if(isWatcher){
    			WatcherRemovedEvent watcherRemovedEvent  = context.getBean(WatcherRemovedEvent.class);
    			watcherRemovedEvent.setUuid(loggedOutEvent.getUuid());
    			watcherRemovedEvent.setArrived(new Date());
    			watcherRemovedEvent.setClazz("WatcherRemovedEvent");
    			gameRooms.get(0).getWatchers().remove(loggedOutEvent.getBackgammonUser().getUserName());
    			watcherRemovedEvent.setGameRoom(gameRooms.get(0));
    			watcherRemovedEvent.setRemovedWatcher(loggedOutEvent.getBackgammonUser().getUserName());
    			
    			logger.info("passing game Room Closed Event to producer...");
    			consumerToProducerWatcherQueue.getEventsQueue().put(watcherRemovedEvent);
    		}
    		else throw new RuntimeException("You need to handle 2nd player...");
    	}
    	else if(gameRooms.size() == 0){
    		logger.info("User is not engaged in any room...");
    	}
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
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

	public ConsumerToProducerQueue getConsumerToProducerWatcherQueue() {
		return consumerToProducerWatcherQueue;
	}

	public void setConsumerToProducerWatcherQueue(ConsumerToProducerQueue consumerToProducerWatcherQueue) {
		this.consumerToProducerWatcherQueue = consumerToProducerWatcherQueue;
	}

	public ConsumerToProducerQueue getConsumerToProducerOpenedByQueue() {
		return consumerToProducerOpenedByQueue;
	}

	public void setConsumerToProducerOpenedByQueue(ConsumerToProducerQueue consumerToProducerOpenedByQueue) {
		this.consumerToProducerOpenedByQueue = consumerToProducerOpenedByQueue;
	}
}
