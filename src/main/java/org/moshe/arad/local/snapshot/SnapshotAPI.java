package org.moshe.arad.local.snapshot;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftLastEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftLastEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftLastEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.OpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.OpenByLeftEvent;
import org.moshe.arad.kafka.events.OpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.OpenByLeftLastEvent;
import org.moshe.arad.kafka.events.SecondLeftEvent;
import org.moshe.arad.kafka.events.SecondLeftFirstEvent;
import org.moshe.arad.kafka.events.SecondLeftLastEvent;
import org.moshe.arad.kafka.events.UserAddedAsSecondPlayerEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
import org.moshe.arad.kafka.events.WatcherLeftEvent;
import org.moshe.arad.kafka.events.WatcherLeftLastEvent;
import org.moshe.arad.kafka.producers.commands.ISimpleCommandProducer;
import org.moshe.arad.kafka.producers.commands.PullEventsWithoutSavingCommandsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class SnapshotAPI implements ApplicationContextAware {
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
	@Autowired
	private ApplicationContext context;
	
	private Map<UUID, LinkedList<BackgammonEvent>> eventsfromMongo = new HashMap<>();
	
	private Map<UUID, Thread> lockers = new HashMap<>(10000);
	
	private Logger logger = LoggerFactory.getLogger(SnapshotAPI.class);
	
	private Set<Object> updateSnapshotLocker = new HashSet<>(100000);
	
	private ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
	
	private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	
	public static final String LAST_UPDATED = "lastUpdateSnapshotDate";
	public static final String GAME_ROOMS = "GameRooms";
	public static final String USERS_OPENED_BY = "UsersOpenedBy";
	
	public boolean isLastUpdateDateExists(){
		try{
			readWriteLock.readLock().lock();			
			return redisTemplate.hasKey(LAST_UPDATED);
		}
		finally{
			readWriteLock.readLock().unlock();
		}		
	}
	
	public Date getLastUpdateDate(){
		if(isLastUpdateDateExists()){
			try{
				readWriteLock.readLock().lock();			
				return new Date(Long.parseLong(stringRedisTemplate.opsForValue().get(LAST_UPDATED)));
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		else return null;
	}

	public void saveLatestSnapshotDate(Date date){
		readWriteLock.writeLock().lock();
		redisTemplate.opsForValue().set(LAST_UPDATED, Long.toString(date.getTime()));
		readWriteLock.writeLock().unlock();
	}
	
	public Snapshot readLatestSnapshot(){
		Snapshot result = context.getBean(Snapshot.class);
		
		if(!isLastUpdateDateExists()) return null;
		else{
			readWriteLock.readLock().lock();
			
			Map<Object, Object> gameRooms = redisTemplate.opsForHash().entries(GAME_ROOMS);
			Map<Object, Object> usersOpenedBy = redisTemplate.opsForHash().entries(USERS_OPENED_BY);
			
			readWriteLock.readLock().unlock();
			
			HashMap<Object, Object> gameRoomsHash = new HashMap<>(gameRooms);
			HashMap<Object, Object> usersOpenedByHash = new HashMap<>(usersOpenedBy);
			
			if(gameRooms != null) result.setRooms(gameRoomsHash);
			if(usersOpenedBy != null) result.setRooms(usersOpenedByHash);
			
			return result;
		}
	}	
	
	public Snapshot doEventsFoldingAndGetInstanceWithoutSaving(){		
		logger.info("Preparing command producer...");
		PullEventsWithoutSavingCommandsProducer pullEventsWithoutSavingCommandsProducer = context.getBean(PullEventsWithoutSavingCommandsProducer.class);
		UUID uuid = initSingleProducer(pullEventsWithoutSavingCommandsProducer, KafkaUtils.LOBBY_SERVICE_PULL_EVENTS_WITHOUT_SAVING_COMMAND_TOPIC);
		threadPoolExecutor.submit(pullEventsWithoutSavingCommandsProducer);
		logger.info("command submitted...");
		
		Thread current = Thread.currentThread();
		
		synchronized (current) {
			try {				
				lockers.put(uuid, current);
				current.wait(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return getInstanceFromEventsFold(eventsfromMongo.get(uuid));
	}
	
	private UUID initSingleProducer(ISimpleCommandProducer producer, String topic) {
		producer.setPeriodic(false);
		producer.setTopic(topic);
		producer.setUuid(UUID.randomUUID());
		return producer.getUuid();
	}
	
	public void updateLatestSnapshot(Snapshot snapshot){
		
		readWriteLock.writeLock().lock();
		
		redisTemplate.expire(USERS_OPENED_BY, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(GAME_ROOMS, 1, TimeUnit.NANOSECONDS);
		redisTemplate.expire(LAST_UPDATED, 1, TimeUnit.NANOSECONDS);
		
		while(redisTemplate.hasKey(GAME_ROOMS) || redisTemplate.hasKey(USERS_OPENED_BY) || redisTemplate.hasKey(LAST_UPDATED)){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		
		redisTemplate.opsForHash().putAll(GAME_ROOMS, snapshot.getRooms());
		redisTemplate.opsForHash().putAll(USERS_OPENED_BY, snapshot.getUsersOpenedBy());
		
		readWriteLock.writeLock().unlock();				
	}
	
	/**
	 * calculate instance after events fold, will return this instance without saving it into Redis.
	 * @return 
	 */
	public Snapshot getInstanceFromEventsFold(LinkedList<BackgammonEvent> fromMongoEventsStoreEventList){
		boolean isLatestSnapshotExists = this.readLatestSnapshot() == null ? false : true;
		Snapshot currentSnapshot = null;
		
		if(isLatestSnapshotExists) currentSnapshot = this.readLatestSnapshot();
		else currentSnapshot = context.getBean(Snapshot.class);

		ListIterator<BackgammonEvent> it = fromMongoEventsStoreEventList.listIterator();
		
		logger.info("Starting to fold events into current state...");
		
		while(it.hasNext()){			
			BackgammonEvent eventToFold = it.next();
			logger.info("Event to fold = " + eventToFold);
			if(eventToFold.getClazz().equals("NewGameRoomOpenedEvent")){
				NewGameRoomOpenedEvent newGameRoomOpenedEvent = (NewGameRoomOpenedEvent)eventToFold;
				GameRoom gameRoom = newGameRoomOpenedEvent.getGameRoom();
				
				ObjectMapper objectMapper = new ObjectMapper();
				String gameRoomJson = null;
				try {
					gameRoomJson = objectMapper.writeValueAsString(gameRoom);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				currentSnapshot.getRooms().put(gameRoom.getName(), gameRoomJson);
				currentSnapshot.getUsersOpenedBy().put(gameRoom.getOpenBy(), gameRoom.getName());
			}
			else if(eventToFold.getClazz().equals("UserAddedAsWatcherEvent")){
				UserAddedAsWatcherEvent userAddedAsWatcherEvent = (UserAddedAsWatcherEvent)eventToFold;
				GameRoom gameRoom = userAddedAsWatcherEvent.getGameRoom();
				if(!gameRoom.getWatchers().contains(userAddedAsWatcherEvent.getUsername())) gameRoom.getWatchers().add(userAddedAsWatcherEvent.getUsername());
					
				ObjectMapper objectMapper = new ObjectMapper();
				String gameRoomJson = null;
				try {
					gameRoomJson = objectMapper.writeValueAsString(gameRoom);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				
				currentSnapshot.getUsersWatchers().put(userAddedAsWatcherEvent.getUsername(), gameRoom.getName());
				
				currentSnapshot.getRooms().put(gameRoom.getName(), gameRoomJson);								
			}
			else if(eventToFold.getClazz().equals("UserAddedAsSecondPlayerEvent")){
				UserAddedAsSecondPlayerEvent userAddedAsSecondPlayerEvent = (UserAddedAsSecondPlayerEvent)eventToFold;
		
				ObjectMapper objectMapper = new ObjectMapper();
				try {
					GameRoom gameRoom = objectMapper.readValue(currentSnapshot.getRooms().get(userAddedAsSecondPlayerEvent.getGameRoom().getName()).toString(), GameRoom.class);
					gameRoom.setSecondPlayer(userAddedAsSecondPlayerEvent.getUsername());
					String gameRoomJson = objectMapper.writeValueAsString(gameRoom);
					currentSnapshot.getRooms().put(userAddedAsSecondPlayerEvent.getGameRoom().getName(), gameRoomJson);
					currentSnapshot.getUsersSecond().put(userAddedAsSecondPlayerEvent.getUsername(), gameRoom.getName());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else if(eventToFold.getClazz().equals("LoggedOutOpenByLeftBeforeGameStartedEvent")){
				LoggedOutOpenByLeftBeforeGameStartedEvent loggedOutOpenByLeftBeforeGameStartedEvent = (LoggedOutOpenByLeftBeforeGameStartedEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(loggedOutOpenByLeftBeforeGameStartedEvent.getLoggedOutUserName());
				currentSnapshot.getRooms().remove(loggedOutOpenByLeftBeforeGameStartedEvent.getGameRoom());
				
			}
			else if(eventToFold.getClazz().equals("GameRoomClosedEvent")){
				GameRoomClosedEvent gameRoomClosedEvent = (GameRoomClosedEvent)eventToFold;
		
				if(currentSnapshot.getUsersOpenedBy().containsKey(gameRoomClosedEvent.getLoggedOutUserName())){
					currentSnapshot.getUsersOpenedBy().remove(gameRoomClosedEvent.getLoggedOutUserName());
				}
				else if(currentSnapshot.getUsersWatchers().containsKey(gameRoomClosedEvent.getLoggedOutUserName())){
					currentSnapshot.getUsersWatchers().remove(gameRoomClosedEvent.getLoggedOutUserName());
				}
				else if(currentSnapshot.getUsersSecond().containsKey(gameRoomClosedEvent.getLoggedOutUserName())){
					currentSnapshot.getUsersSecond().remove(gameRoomClosedEvent.getLoggedOutUserName());
				}
				currentSnapshot.getRooms().remove(gameRoomClosedEvent.getGameRoom().getName());
			}
			else if(eventToFold.getClazz().equals("LoggedOutOpenByLeftEvent")){
				LoggedOutOpenByLeftEvent loggedOutOpenByLeftEvent = (LoggedOutOpenByLeftEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(loggedOutOpenByLeftEvent.getOpenBy());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutOpenByLeftEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setOpenBy("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("LoggedOutWatcherLeftLastEvent")){
				LoggedOutWatcherLeftLastEvent loggedOutWatcherLeftLastEvent = (LoggedOutWatcherLeftLastEvent)eventToFold;
				
				currentSnapshot.getUsersWatchers().remove(loggedOutWatcherLeftLastEvent.getWatcher());
				currentSnapshot.getRooms().remove(loggedOutWatcherLeftLastEvent.getGameRoom().getName());			
			}
			else if(eventToFold.getClazz().equals("LoggedOutWatcherLeftEvent")){
				LoggedOutWatcherLeftEvent loggedOutWatcherLeftEvent = (LoggedOutWatcherLeftEvent)eventToFold;
				
				currentSnapshot.getUsersWatchers().remove(loggedOutWatcherLeftEvent.getWatcher());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutWatcherLeftEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.getWatchers().remove(loggedOutWatcherLeftEvent.getWatcher());
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("LoggedOutOpenByLeftFirstEvent")){
				LoggedOutOpenByLeftFirstEvent loggedOutOpenByLeftFirstEvent = (LoggedOutOpenByLeftFirstEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(loggedOutOpenByLeftFirstEvent.getOpenBy());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutOpenByLeftFirstEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setOpenBy("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("LoggedOutSecondLeftFirstEvent")){
				LoggedOutSecondLeftFirstEvent loggedOutSecondLeftFirstEvent = (LoggedOutSecondLeftFirstEvent)eventToFold;
				
				currentSnapshot.getUsersSecond().remove(loggedOutSecondLeftFirstEvent.getSecond());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutSecondLeftFirstEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setSecondPlayer("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("LoggedOutSecondLeftEvent")){
				LoggedOutSecondLeftEvent loggedOutSecondLeftEvent = (LoggedOutSecondLeftEvent)eventToFold;
				
				currentSnapshot.getUsersSecond().remove(loggedOutSecondLeftEvent.getSecond());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutSecondLeftEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setSecondPlayer("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("LoggedOutOpenByLeftLastEvent")){
				LoggedOutOpenByLeftLastEvent loggedOutOpenByLeftLastEvent = (LoggedOutOpenByLeftLastEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(loggedOutOpenByLeftLastEvent.getOpenBy());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutOpenByLeftLastEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setOpenBy("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("LoggedOutSecondLeftLastEvent")){
				LoggedOutSecondLeftLastEvent loggedOutSecondLeftLastEvent = (LoggedOutSecondLeftLastEvent)eventToFold;
				
				currentSnapshot.getUsersSecond().remove(loggedOutSecondLeftLastEvent.getSecond());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(loggedOutSecondLeftLastEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setSecondPlayer("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("OpenByLeftBeforeGameStartedEvent")){
				OpenByLeftBeforeGameStartedEvent openByLeftBeforeGameStartedEvent = (OpenByLeftBeforeGameStartedEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(openByLeftBeforeGameStartedEvent.getLeavingUserName());
				currentSnapshot.getRooms().remove(openByLeftBeforeGameStartedEvent.getGameRoom().getName());			
			}
			else if(eventToFold.getClazz().equals("OpenByLeftEvent")){
				OpenByLeftEvent openByLeftEvent = (OpenByLeftEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(openByLeftEvent.getOpenBy());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(openByLeftEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setOpenBy("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("WatcherLeftLastEvent")){
				WatcherLeftLastEvent watcherLeftLastEvent = (WatcherLeftLastEvent)eventToFold;
				
				currentSnapshot.getUsersWatchers().remove(watcherLeftLastEvent.getWatcher());
				currentSnapshot.getRooms().remove(watcherLeftLastEvent.getGameRoom().getName());				
			}
			else if(eventToFold.getClazz().equals("WatcherLeftEvent")){
				WatcherLeftEvent watcherLeftEvent = (WatcherLeftEvent)eventToFold;
				
				currentSnapshot.getUsersWatchers().remove(watcherLeftEvent.getWatcher());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(watcherLeftEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.getWatchers().remove(watcherLeftEvent.getWatcher());
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("OpenByLeftFirstEvent")){
				OpenByLeftFirstEvent openByLeftFirstEvent = (OpenByLeftFirstEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(openByLeftFirstEvent.getOpenBy());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(openByLeftFirstEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setOpenBy("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("SecondLeftFirstEvent")){
				SecondLeftFirstEvent secondLeftFirstEvent = (SecondLeftFirstEvent)eventToFold;
				
				currentSnapshot.getUsersSecond().remove(secondLeftFirstEvent.getSecond());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(secondLeftFirstEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setSecondPlayer("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("SecondLeftEvent")){
				SecondLeftEvent secondLeftEvent = (SecondLeftEvent)eventToFold;
				
				currentSnapshot.getUsersSecond().remove(secondLeftEvent.getSecond());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(secondLeftEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setSecondPlayer("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("OpenByLeftLastEvent")){
				OpenByLeftLastEvent openByLeftLastEvent = (OpenByLeftLastEvent)eventToFold;
				
				currentSnapshot.getUsersOpenedBy().remove(openByLeftLastEvent.getOpenBy());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(openByLeftLastEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setOpenBy("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			else if(eventToFold.getClazz().equals("SecondLeftLastEvent")){
				SecondLeftLastEvent secondLeftLastEvent = (SecondLeftLastEvent)eventToFold;
				
				currentSnapshot.getUsersSecond().remove(secondLeftLastEvent.getSecond());
				ObjectMapper objectMapper = new ObjectMapper();				
				GameRoom room = null;
				try {
					room = objectMapper.readValue(currentSnapshot.getRooms().get(secondLeftLastEvent.getGameRoom().getName()).toString(), GameRoom.class);
					room.setSecondPlayer("left");
					String roomJson = objectMapper.writeValueAsString(room);
					currentSnapshot.getRooms().put(room.getName(), roomJson);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
			
			logger.info("Event to folded successfuly = " + eventToFold);
		}
		
		logger.info("Events folding into current state completed...");
		
		fromMongoEventsStoreEventList.clear();
		return currentSnapshot;
	}
	
	public Set<Object> getUpdateSnapshotLocker() {
		return updateSnapshotLocker;
	}

	public void setUpdateSnapshotLocker(Set<Object> updateSnapshotLocker) {
		this.updateSnapshotLocker = updateSnapshotLocker;
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}

	public Map<UUID, Thread> getLockers() {
		return lockers;
	}

	public void setLockers(Map<UUID, Thread> lockers) {
		this.lockers = lockers;
	}

	public Map<UUID, LinkedList<BackgammonEvent>> getEventsfromMongo() {
		return eventsfromMongo;
	}

	public void setEventsfromMongo(Map<UUID, LinkedList<BackgammonEvent>> eventsfromMongo) {
		this.eventsfromMongo = eventsfromMongo;
	}
}
