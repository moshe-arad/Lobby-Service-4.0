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
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutUserLeftLobbyEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftLastEvent;
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
    		else if(rooms.size() == 1){
    			GameRoom room = rooms.get(0);
    			
    			boolean isWatcherLogout = room.getWatchers().contains(loggedOutEvent) ? true : false;
    			boolean isOpenByLogout = room.getOpenBy().equals(loggedOutEvent) ? true : false;
    			boolean isSecondLogout = room.getSecondPlayer().equals(loggedOutEvent) ? true : false;
    					
    			if(!room.getOpenBy().equals("left") && !room.getOpenBy().isEmpty() && room.getSecondPlayer().isEmpty() && room.getWatchers().size() == 0){
    				logger.info("User is engaged in a room which he's the only participant in it...");
    				logger.info("User will try to leave this room...");
    				
    				LoggedOutOpenByLeftBeforeGameStartedEvent loggedOutOpenByLeftBeforeGameStartedEvent = context.getBean(LoggedOutOpenByLeftBeforeGameStartedEvent.class);
    				loggedOutOpenByLeftBeforeGameStartedEvent.setUuid(loggedOutEvent.getUuid());
    				loggedOutOpenByLeftBeforeGameStartedEvent.setArrived(new Date());
    				loggedOutOpenByLeftBeforeGameStartedEvent.setClazz("LoggedOutOpenByLeftBeforeGameStartedEvent");
    				loggedOutOpenByLeftBeforeGameStartedEvent.setLoggedOutUserName(user.getUserName());
    				room.setOpenBy("left");
    				loggedOutOpenByLeftBeforeGameStartedEvent.setGameRoom(room);
    				
    				consumerToProducer.get(LoggedOutOpenByLeftBeforeGameStartedEvent.class).getEventsQueue().put(loggedOutOpenByLeftBeforeGameStartedEvent);
    			}
    			else if(isOpenByLogout && 
    					(!room.getOpenBy().equals("left") && !room.getOpenBy().isEmpty() && room.getSecondPlayer().isEmpty() && room.getWatchers().size() > 0) ||
    					(!room.getOpenBy().equals("left") && !room.getOpenBy().isEmpty() && room.getSecondPlayer().equals("left") && room.getWatchers().size() > 0)){
    				logger.info("User is engaged in a room which second player has left or did not joined yet, plus this game room has watchers...");
    				logger.info("User will try to leave this room...");
    				
    				LoggedOutOpenByLeftEvent loggedOutOpenByLeftEvent = context.getBean(LoggedOutOpenByLeftEvent.class);
    				loggedOutOpenByLeftEvent.setUuid(loggedOutEvent.getUuid());
    				loggedOutOpenByLeftEvent.setArrived(new Date());
    				loggedOutOpenByLeftEvent.setClazz("LoggedOutOpenByLeftEvent");
    				loggedOutOpenByLeftEvent.setOpenBy(room.getOpenBy());
    				room.setOpenBy("left");
    				loggedOutOpenByLeftEvent.setGameRoom(room);
    				
    				consumerToProducer.get(LoggedOutOpenByLeftEvent.class).getEventsQueue().put(loggedOutOpenByLeftEvent);
    			}
    			else if(isWatcherLogout && 
    					(room.getOpenBy().equals("left") && room.getSecondPlayer().isEmpty() && room.getWatchers().size() == 1) ||
    					(room.getOpenBy().equals("left") && room.getSecondPlayer().equals("left") && room.getWatchers().size() == 1)){
    				logger.info("User is engaged in a room which he is the last watcher and participant...");
    				logger.info("User will try to leave this room...");
    				
    				LoggedOutWatcherLeftLastEvent loggedOutWatcherLeftLastEvent = context.getBean(LoggedOutWatcherLeftLastEvent.class);
    				loggedOutWatcherLeftLastEvent.setUuid(loggedOutEvent.getUuid());
    				loggedOutWatcherLeftLastEvent.setArrived(new Date());
    				loggedOutWatcherLeftLastEvent.setClazz("LoggedOutWatcherLeftLastEvent");
    				loggedOutWatcherLeftLastEvent.setWatcher(user.getUserName());
    				room.getWatchers().remove(user.getUserName());
    				loggedOutWatcherLeftLastEvent.setGameRoom(room);
    				
    				consumerToProducer.get(LoggedOutWatcherLeftLastEvent.class).getEventsQueue().put(loggedOutWatcherLeftLastEvent);
    			}
    			else if(isWatcherLogout &&
    					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && !room.getSecondPlayer().isEmpty() && !room.getSecondPlayer().equals("left") && room.getWatchers().size() > 0) ||
    					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && room.getSecondPlayer().isEmpty() && room.getWatchers().size() > 0) ||
    					(room.getOpenBy().equals("left") && room.getSecondPlayer().isEmpty() && room.getWatchers().size() > 1) ||
    					(room.getOpenBy().equals("left") && room.getSecondPlayer().equals("left") && room.getWatchers().size() > 1)){
    				logger.info("User is engaged in a room as a watcher...");
    				logger.info("User will try to leave this room...");
    				
    				LoggedOutWatcherLeftEvent loggedOutWatcherLeftEvent = context.getBean(LoggedOutWatcherLeftEvent.class);
    				loggedOutWatcherLeftEvent.setUuid(loggedOutEvent.getUuid());
    				loggedOutWatcherLeftEvent.setArrived(new Date());
    				loggedOutWatcherLeftEvent.setClazz("LoggedOutWatcherLeftEvent");
    				loggedOutWatcherLeftEvent.setWatcher(user.getUserName());
    				room.getWatchers().remove(user.getUserName());
    				loggedOutWatcherLeftEvent.setGameRoom(room);
    				
    				consumerToProducer.get(LoggedOutWatcherLeftEvent.class).getEventsQueue().put(loggedOutWatcherLeftEvent);
    			}
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
