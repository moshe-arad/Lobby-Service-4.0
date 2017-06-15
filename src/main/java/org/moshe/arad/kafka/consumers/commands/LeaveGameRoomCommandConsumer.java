package org.moshe.arad.kafka.consumers.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.LeaveGameRoomCommand;
import org.moshe.arad.kafka.commands.OpenNewGameRoomCommand;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftLastEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftLastEvent;
import org.moshe.arad.kafka.events.LoggedOutUserLeftLobbyEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftLastEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEventAck;
import org.moshe.arad.kafka.events.OpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.OpenByLeftEvent;
import org.moshe.arad.kafka.events.OpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.SecondLeftEvent;
import org.moshe.arad.kafka.events.SecondLeftFirstEvent;
import org.moshe.arad.kafka.events.WatcherLeftEvent;
import org.moshe.arad.kafka.events.WatcherLeftLastEvent;
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
public class LeaveGameRoomCommandConsumer extends SimpleCommandsConsumer{

	@Autowired
	private LobbyRepository lobbyRepository;
	
	@Autowired
	private ApplicationContext context;
	
	private Map<Class<? extends BackgammonEvent>,ConsumerToProducerQueue> consumerToProducer;
	
	private Logger logger = LoggerFactory.getLogger(LeaveGameRoomCommandConsumer.class);
	
	@Override
	public void consumerOperations(ConsumerRecord<String, String> record) {
		LeaveGameRoomCommand leaveGameRoomCommand = convertJsonBlobIntoEvent(record.value()); 
		String userName = leaveGameRoomCommand.getUsername();
		List<GameRoom> rooms = lobbyRepository.getUserEngagedGameRooms(userName);
		
		if(rooms.size() == 1){
			GameRoom room = rooms.get(0);
			
			boolean isWatcherLeaving = room.getWatchers().contains(userName) ? true : false;
			boolean isOpenByLeaving = room.getOpenBy().equals(userName) ? true : false;
			boolean isSecondLeaving = room.getSecondPlayer().equals(userName) ? true : false;
					
			if(!room.getOpenBy().equals("left") && !room.getOpenBy().isEmpty() && room.getSecondPlayer().isEmpty() && room.getWatchers().size() == 0){
				logger.info("User is engaged in a room which he's the only participant in it...");
				logger.info("User will try to leave this room...");
				
				OpenByLeftBeforeGameStartedEvent openByLeftBeforeGameStartedEvent = context.getBean(OpenByLeftBeforeGameStartedEvent.class);
				openByLeftBeforeGameStartedEvent.setUuid(leaveGameRoomCommand.getUuid());
				openByLeftBeforeGameStartedEvent.setArrived(new Date());
				openByLeftBeforeGameStartedEvent.setClazz("OpenByLeftBeforeGameStartedEvent");
				openByLeftBeforeGameStartedEvent.setLeavingUserName(userName);
				room.setOpenBy("left");
				openByLeftBeforeGameStartedEvent.setGameRoom(room);
				
				consumerToProducer.get(LoggedOutOpenByLeftBeforeGameStartedEvent.class).getEventsQueue().put(openByLeftBeforeGameStartedEvent);
			}
			else if(isOpenByLeaving && 
					((!room.getOpenBy().equals("left") && !room.getOpenBy().isEmpty() && room.getSecondPlayer().isEmpty() && room.getWatchers().size() > 0) ||
					(!room.getOpenBy().equals("left") && !room.getOpenBy().isEmpty() && room.getSecondPlayer().equals("left") && room.getWatchers().size() > 0))){
				logger.info("User is engaged in a room which second player has left or did not joined yet, plus this game room has watchers...");
				logger.info("User will try to leave this room...");
				
				OpenByLeftEvent openByLeftEvent = context.getBean(OpenByLeftEvent.class);
				openByLeftEvent.setUuid(leaveGameRoomCommand.getUuid());
				openByLeftEvent.setArrived(new Date());
				openByLeftEvent.setClazz("OpenByLeftEvent");
				openByLeftEvent.setOpenBy(room.getOpenBy());
				room.setOpenBy("left");
				openByLeftEvent.setGameRoom(room);
				
				consumerToProducer.get(OpenByLeftEvent.class).getEventsQueue().put(openByLeftEvent);
			}
			else if(isWatcherLeaving && 
					((room.getOpenBy().equals("left") && room.getSecondPlayer().isEmpty() && room.getWatchers().size() == 1) ||
					(room.getOpenBy().equals("left") && room.getSecondPlayer().equals("left") && room.getWatchers().size() == 1))){
				logger.info("User is engaged in a room which he is the last watcher and participant...");
				logger.info("User will try to leave this room...");
				
				WatcherLeftLastEvent watcherLeftLastEvent = context.getBean(WatcherLeftLastEvent.class);
				watcherLeftLastEvent.setUuid(leaveGameRoomCommand.getUuid());
				watcherLeftLastEvent.setArrived(new Date());
				watcherLeftLastEvent.setClazz("WatcherLeftLastEvent");
				watcherLeftLastEvent.setWatcher(userName);
				room.getWatchers().remove(userName);
				watcherLeftLastEvent.setGameRoom(room);
				
				consumerToProducer.get(WatcherLeftLastEvent.class).getEventsQueue().put(watcherLeftLastEvent);
			}
			else if(isWatcherLeaving &&
					((!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && !room.getSecondPlayer().isEmpty() && !room.getSecondPlayer().equals("left") && room.getWatchers().size() > 0) ||
					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && room.getSecondPlayer().isEmpty() && room.getWatchers().size() > 0) ||
					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && room.getSecondPlayer().equals("left") && room.getWatchers().size() > 0) ||
					(room.getOpenBy().equals("left") && room.getSecondPlayer().isEmpty() && room.getWatchers().size() > 1) ||
					(room.getOpenBy().equals("left") && room.getSecondPlayer().equals("left") && room.getWatchers().size() > 1))){
				logger.info("User is engaged in a room as a watcher...");
				logger.info("User will try to leave this room...");
				
				WatcherLeftEvent watcherLeftEvent = context.getBean(WatcherLeftEvent.class);
				watcherLeftEvent.setUuid(leaveGameRoomCommand.getUuid());
				watcherLeftEvent.setArrived(new Date());
				watcherLeftEvent.setClazz("WatcherLeftEvent");
				watcherLeftEvent.setWatcher(userName);
				room.getWatchers().remove(userName);
				watcherLeftEvent.setGameRoom(room);
				
				consumerToProducer.get(WatcherLeftEvent.class).getEventsQueue().put(watcherLeftEvent);
			}
			else if(isOpenByLeaving && 
					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && !room.getSecondPlayer().isEmpty() && !room.getSecondPlayer().equals("left"))){
				logger.info("User is engaged in a room as a openBy, and game as already began...");
				logger.info("User will try to leave this room...");
				
				OpenByLeftFirstEvent openByLeftFirstEvent = context.getBean(OpenByLeftFirstEvent.class);
				openByLeftFirstEvent.setUuid(leaveGameRoomCommand.getUuid());
				openByLeftFirstEvent.setArrived(new Date());
				openByLeftFirstEvent.setClazz("OpenByLeftFirstEvent");
				openByLeftFirstEvent.setOpenBy(userName);
				room.setOpenBy("left");
				openByLeftFirstEvent.setGameRoom(room);
				
				consumerToProducer.get(OpenByLeftFirstEvent.class).getEventsQueue().put(openByLeftFirstEvent);
			}
			else if(isSecondLeaving && 
					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && !room.getSecondPlayer().isEmpty() && !room.getSecondPlayer().equals("left"))){
				logger.info("User is engaged in a room as a second player, and game as already began...");
				logger.info("User will try to leave this room...");
				
				SecondLeftFirstEvent secondLeftFirstEvent = context.getBean(SecondLeftFirstEvent.class);
				secondLeftFirstEvent.setUuid(leaveGameRoomCommand.getUuid());
				secondLeftFirstEvent.setArrived(new Date());
				secondLeftFirstEvent.setClazz("SecondLeftFirstEvent");
				secondLeftFirstEvent.setSecond(userName);
				room.setSecondPlayer("left");
				secondLeftFirstEvent.setGameRoom(room);
				
				consumerToProducer.get(SecondLeftFirstEvent.class).getEventsQueue().put(secondLeftFirstEvent);
			}
			else if(isSecondLeaving && 
					(room.getOpenBy().equals("left") && !room.getSecondPlayer().isEmpty()  && !room.getSecondPlayer().equals("left") && room.getWatchers().size() > 0)){
				logger.info("User is engaged in a room as a second player, first player already left and room has watchers...");
				logger.info("User will try to leave this room...");
				
				SecondLeftEvent secondLeftEvent = context.getBean(SecondLeftEvent.class);
				secondLeftEvent.setUuid(leaveGameRoomCommand.getUuid());
				secondLeftEvent.setArrived(new Date());
				secondLeftEvent.setClazz("SecondLeftEvent");
				secondLeftEvent.setSecond(userName);
				room.setSecondPlayer("left");
				secondLeftEvent.setGameRoom(room);
				
				consumerToProducer.get(SecondLeftEvent.class).getEventsQueue().put(secondLeftEvent);
			}
//			else if(isOpenByLeaving && 
//					(!room.getOpenBy().isEmpty() && !room.getOpenBy().equals("left") && room.getSecondPlayer().equals("left") && room.getWatchers().size() == 0)){
//				logger.info("User is engaged in a room as openBy player, second player already left and room has no watchers...");
//				logger.info("User will try to leave this room...");
//				
//				LoggedOutOpenByLeftLastEvent loggedOutOpenByLeftLastEvent = context.getBean(LoggedOutOpenByLeftLastEvent.class);
//				loggedOutOpenByLeftLastEvent.setUuid(loggedOutEvent.getUuid());
//				loggedOutOpenByLeftLastEvent.setArrived(new Date());
//				loggedOutOpenByLeftLastEvent.setClazz("LoggedOutOpenByLeftLastEvent");
//				loggedOutOpenByLeftLastEvent.setOpenBy(user.getUserName());
//				room.setOpenBy("left");
//				loggedOutOpenByLeftLastEvent.setGameRoom(room);
//				
//				consumerToProducer.get(LoggedOutOpenByLeftLastEvent.class).getEventsQueue().put(loggedOutOpenByLeftLastEvent);
//			}
//			else if(isSecondLeaving && 
//					(room.getOpenBy().equals("left") && !room.getSecondPlayer().isEmpty() && !room.getSecondPlayer().equals("left") && room.getWatchers().size() == 0)){
//				logger.info("User is engaged in a room as second player, openBy player already left and room has no watchers...");
//				logger.info("User will try to leave this room...");
//				
//				LoggedOutSecondLeftLastEvent loggedOutSecondLeftLastEvent = context.getBean(LoggedOutSecondLeftLastEvent.class);
//				loggedOutSecondLeftLastEvent.setUuid(loggedOutEvent.getUuid());
//				loggedOutSecondLeftLastEvent.setArrived(new Date());
//				loggedOutSecondLeftLastEvent.setClazz("LoggedOutSecondLeftLastEvent");
//				loggedOutSecondLeftLastEvent.setSecond(user.getUserName());
//				room.setSecondPlayer("left");
//				loggedOutSecondLeftLastEvent.setGameRoom(room);
//				
//				consumerToProducer.get(LoggedOutSecondLeftLastEvent.class).getEventsQueue().put(loggedOutSecondLeftLastEvent);
//			}
			else throw new RuntimeException("Leaving user, failed to calculate engaged rooms...");
		}
		else throw new RuntimeException("Leaving user, failed to calculate engaged rooms...");
	}

	private LeaveGameRoomCommand convertJsonBlobIntoEvent(String JsonBlob){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.readValue(JsonBlob, LeaveGameRoomCommand.class);
		} catch (IOException e) {
			logger.error("Falied to convert Json blob into Event...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	public Map<Class<? extends BackgammonEvent>, ConsumerToProducerQueue> getConsumerToProducer() {
		return consumerToProducer;
	}

	public void setConsumerToProducer(Map<Class<? extends BackgammonEvent>, ConsumerToProducerQueue> consumerToProducer) {
		this.consumerToProducer = consumerToProducer;
	}

	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		
	}
}
