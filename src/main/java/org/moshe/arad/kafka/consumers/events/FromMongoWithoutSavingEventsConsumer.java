package org.moshe.arad.kafka.consumers.events;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.EventsBasketFromMongo;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftLastEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.UserAddedAsSecondPlayerEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Scope("prototype")
public class FromMongoWithoutSavingEventsConsumer extends SimpleEventsConsumer {
	
	@Autowired
	private SnapshotAPI snapshotAPI;
	
	Logger logger = LoggerFactory.getLogger(FromMongoWithoutSavingEventsConsumer.class);
	
	@Autowired
	private EventsBasketFromMongo eventsBasketFromMongo;
	
	int substrcatEventsNum = 0;
	
	public FromMongoWithoutSavingEventsConsumer() {

	}

	@Override
	public void consumerOperations(ConsumerRecord<String,String> record) {
		try{
			logger.info("Trying to convert JSON blob to Backgammon Event record, JSON blob = " + record.value());
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readValue(record.value(), JsonNode.class);
			String clazz = jsonNode.get("clazz").asText();
			String uuid = jsonNode.get("uuid").asText();
			BackgammonEvent backgammonEvent = null;			
			
			if(clazz.equals("StartReadEventsFromMongoEvent"))
			{
				logger.info("Recieved the begin read events record, starting reading events from events store...");
				int tempTotalNumOfEvents = jsonNode.get("totalNumOfEvents").asInt();				
				eventsBasketFromMongo.putTotalNumOfEventsFor(uuid, tempTotalNumOfEvents);											
			}
			else if(clazz.equals("NewGameRoomOpenedEvent")){
				NewGameRoomOpenedEvent newGameRoomOpenedEvent = objectMapper.readValue(record.value(), NewGameRoomOpenedEvent.class);
				backgammonEvent = newGameRoomOpenedEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("UserAddedAsWatcherEvent")){
				UserAddedAsWatcherEvent userAddedAsWatcherEvent = objectMapper.readValue(record.value(), UserAddedAsWatcherEvent.class);
				backgammonEvent = userAddedAsWatcherEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("UserAddedAsSecondPlayerEvent")){
				UserAddedAsSecondPlayerEvent userAddedAsSecondPlayerEvent = objectMapper.readValue(record.value(), UserAddedAsSecondPlayerEvent.class);
				backgammonEvent = userAddedAsSecondPlayerEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedOutOpenByLeftBeforeGameStartedEvent")){
				LoggedOutOpenByLeftBeforeGameStartedEvent loggedOutOpenByLeftBeforeGameStartedEvent = objectMapper.readValue(record.value(), LoggedOutOpenByLeftBeforeGameStartedEvent.class);
				backgammonEvent = loggedOutOpenByLeftBeforeGameStartedEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("GameRoomClosedEvent")){
				GameRoomClosedEvent gameRoomClosedEvent = objectMapper.readValue(record.value(), GameRoomClosedEvent.class);
				backgammonEvent = gameRoomClosedEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedOutOpenByLeftEvent")){
				LoggedOutOpenByLeftEvent loggedOutOpenByLeftEvent = objectMapper.readValue(record.value(), LoggedOutOpenByLeftEvent.class);
				backgammonEvent = loggedOutOpenByLeftEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedOutWatcherLeftLastEvent")){
				LoggedOutWatcherLeftLastEvent loggedOutWatcherLeftLastEvent = objectMapper.readValue(record.value(), LoggedOutWatcherLeftLastEvent.class);
				backgammonEvent = loggedOutWatcherLeftLastEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedOutWatcherLeftEvent")){
				LoggedOutWatcherLeftEvent loggedOutWatcherLeftEvent = objectMapper.readValue(record.value(), LoggedOutWatcherLeftEvent.class);
				backgammonEvent = loggedOutWatcherLeftEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedOutOpenByLeftFirstEvent")){
				LoggedOutOpenByLeftFirstEvent loggedOutOpenByLeftFirstEvent = objectMapper.readValue(record.value(), LoggedOutOpenByLeftFirstEvent.class);
				backgammonEvent = loggedOutOpenByLeftFirstEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedOutSecondLeftFirstEvent")){
				LoggedOutSecondLeftFirstEvent loggedOutSecondLeftFirstEvent = objectMapper.readValue(record.value(), LoggedOutSecondLeftFirstEvent.class);
				backgammonEvent = loggedOutSecondLeftFirstEvent;

				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}

			if(eventsBasketFromMongo.isReadyHandleEventsFromMongo(uuid)){
				logger.info("Updating SnapshotAPI with collected events data from mongo events store...");
				
				//put events in snapshotAPI
				LinkedList<BackgammonEvent> events = getSortedListByArrivedDate(uuid);
				Thread locker = snapshotAPI.getLockers().get(UUID.fromString(uuid));
				synchronized (locker) {
					snapshotAPI.getEventsfromMongo().put(UUID.fromString(uuid), events);
					locker.notifyAll();
				}
				
				substrcatEventsNum = 0;
				eventsBasketFromMongo.cleanByUuid(uuid);
			}
		}
		catch(Exception ex){
			logger.error("Failed to save data into redis...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	public LinkedList<BackgammonEvent> getSortedListByArrivedDate(String uuid){
		ArrayList<BackgammonEvent> fromMongoEventsStoreEventList = new ArrayList<>(eventsBasketFromMongo.getEvents(uuid));
		fromMongoEventsStoreEventList = (ArrayList<BackgammonEvent>) fromMongoEventsStoreEventList.stream().sorted((BackgammonEvent e1, BackgammonEvent e2) -> {return e1.getArrived().compareTo(e2.getArrived());}).collect(Collectors.toList());
		return new LinkedList<>(fromMongoEventsStoreEventList);
	}

	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		// TODO Auto-generated method stub
		
	}
}




	