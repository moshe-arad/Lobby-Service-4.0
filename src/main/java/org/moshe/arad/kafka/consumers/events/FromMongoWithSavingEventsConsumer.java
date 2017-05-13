package org.moshe.arad.kafka.consumers.events;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.EventsBasketFromMongo;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.LoggedInEvent;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
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
public class FromMongoWithSavingEventsConsumer extends SimpleEventsConsumer {
	
	@Autowired
	private SnapshotAPI snapshotAPI;
	
	Logger logger = LoggerFactory.getLogger(FromMongoWithSavingEventsConsumer.class);
	
	@Autowired
	private EventsBasketFromMongo eventsBasketFromMongo;
	
	public FromMongoWithSavingEventsConsumer() {

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
			else if(clazz.equals("NewUserCreatedEvent")){
				NewUserCreatedEvent newUserCreatedEvent = objectMapper.readValue(record.value(), NewUserCreatedEvent.class);
				backgammonEvent = newUserCreatedEvent;
		
				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("NewUserJoinedLobbyEvent")){
				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = objectMapper.readValue(record.value(), NewUserJoinedLobbyEvent.class);
				backgammonEvent = newUserJoinedLobbyEvent;
				
				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedInEvent")){
				LoggedInEvent loggedInEvent = objectMapper.readValue(record.value(), LoggedInEvent.class);
				backgammonEvent = loggedInEvent;
				
				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("ExistingUserJoinedLobbyEvent")){
				ExistingUserJoinedLobbyEvent existingUserJoinedLobbyEvent = objectMapper.readValue(record.value(), ExistingUserJoinedLobbyEvent.class);
				backgammonEvent = existingUserJoinedLobbyEvent;
				
				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
			}
//			else if(clazz.equals("LoggedOutEvent")){
//				LogoutUserEvent logoutUserEvent = objectMapper.readValue(record.value(), LogoutUserEvent.class);
//				backgammonEvent = logoutUserEvent;
//				
//				eventsBasketFromMongo.addEventToCollectedEvents(uuid, backgammonEvent);
//			}
		
			if(eventsBasketFromMongo.isReadyHandleEventsFromMongo(uuid)){
				logger.info("Updating SnapshotAPI with collected events data from mongo events store...");
				
				//do save
				LinkedList<BackgammonEvent> sortedEvents = getSortedListByArrivedDate(uuid);
				Date latestEventDate = sortedEvents.getLast().getArrived();
			
				snapshotAPI.updateLatestSnapshot(snapshotAPI.getInstanceFromEventsFold(sortedEvents));
				snapshotAPI.saveLatestSnapshotDate(latestEventDate);
				logger.info("SnapshotAPI updated...");
				
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




	