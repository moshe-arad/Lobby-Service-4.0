package org.moshe.arad.kafka.consumers.events;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
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
public class FromMongoWithoutSavingEventsConsumer extends SimpleEventsConsumer {
	
	@Autowired
	private SnapshotAPI snapshotAPI;
	
	Logger logger = LoggerFactory.getLogger(FromMongoWithoutSavingEventsConsumer.class);
	
	private Map<UUID,Set<BackgammonEvent>> eventsMap = new HashMap<>(100000);
	private Map<UUID,Integer> totalNumOfEvents = new HashMap<>(100000);
	
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
				totalNumOfEvents.put(UUID.fromString(uuid), Integer.valueOf(tempTotalNumOfEvents));
				
				if(tempTotalNumOfEvents == 0) eventsMap.put(UUID.fromString(uuid), new HashSet<>());
			}
			else if(clazz.equals("NewUserCreatedEvent")){
				NewUserCreatedEvent newUserCreatedEvent = objectMapper.readValue(record.value(), NewUserCreatedEvent.class);
				backgammonEvent = newUserCreatedEvent;

				addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("NewUserJoinedLobbyEvent")){
				NewUserJoinedLobbyEvent newUserJoinedLobbyEvent = objectMapper.readValue(record.value(), NewUserJoinedLobbyEvent.class);
				backgammonEvent = newUserJoinedLobbyEvent;

				addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("LoggedInEvent")){
				LoggedInEvent loggedInEvent = objectMapper.readValue(record.value(), LoggedInEvent.class);
				backgammonEvent = loggedInEvent;
				
				addEventToCollectedEvents(uuid, backgammonEvent);
			}
			else if(clazz.equals("ExistingUserJoinedLobbyEvent")){
				ExistingUserJoinedLobbyEvent existingUserJoinedLobbyEvent = objectMapper.readValue(record.value(), ExistingUserJoinedLobbyEvent.class);
				backgammonEvent = existingUserJoinedLobbyEvent;
				
				addEventToCollectedEvents(uuid, backgammonEvent);
			}
//			else if(clazz.equals("LoggedOutEvent")){
//				LogoutUserEvent logoutUserEvent = objectMapper.readValue(record.value(), LogoutUserEvent.class);
//				backgammonEvent = logoutUserEvent;
//				
//				addEventToCollectedEvents(uuid, backgammonEvent);
//			}
			
			if(totalNumOfEvents.get(UUID.fromString(uuid)) != null && eventsMap.get(UUID.fromString(uuid)) != null && eventsMap.get(UUID.fromString(uuid)).size() == totalNumOfEvents.get(UUID.fromString(uuid))){
				logger.info("Updating SnapshotAPI with collected events data from mongo events store...");
				
				//put events in snapshotAPI
				LinkedList<BackgammonEvent> events = getSortedListByArrivedDate(uuid);
				Thread locker = snapshotAPI.getLockers().get(UUID.fromString(uuid));
				synchronized (locker) {
					snapshotAPI.getEventsfromMongo().put(UUID.fromString(uuid), events);
					locker.notifyAll();
				}
							
				totalNumOfEvents.remove(UUID.fromString(uuid));
				eventsMap.remove(UUID.fromString(uuid));
			}
		}
		catch(Exception ex){
			logger.error("Failed to save data into redis...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	public LinkedList<BackgammonEvent> getSortedListByArrivedDate(String uuid){
		ArrayList<BackgammonEvent> fromMongoEventsStoreEventList = new ArrayList<>(eventsMap.get(UUID.fromString(uuid)));
		fromMongoEventsStoreEventList = (ArrayList<BackgammonEvent>) fromMongoEventsStoreEventList.stream().sorted((BackgammonEvent e1, BackgammonEvent e2) -> {return e1.getArrived().compareTo(e2.getArrived());}).collect(Collectors.toList());
		return new LinkedList<>(fromMongoEventsStoreEventList);
	}
	
	private void addEventToCollectedEvents(String uuid, BackgammonEvent backgammonEvent) {
		if(!eventsMap.containsKey(UUID.fromString(uuid))){
			Set<BackgammonEvent> eventsSet = new HashSet<>(100000);
			eventsSet.add(backgammonEvent);
			eventsMap.put(UUID.fromString(uuid), eventsSet);
		}
		else eventsMap.get(UUID.fromString(uuid)).add(backgammonEvent);
	}

	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		// TODO Auto-generated method stub
		
	}
}




	