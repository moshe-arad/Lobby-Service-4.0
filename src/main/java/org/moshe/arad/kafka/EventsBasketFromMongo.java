package org.moshe.arad.kafka;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;

import org.moshe.arad.kafka.consumers.events.FromMongoWithSavingEventsConsumer;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EventsBasketFromMongo {

	private Map<UUID,Set<BackgammonEvent>> eventsMap = new ConcurrentHashMap<>(100000);
	private Map<UUID,Integer> totalNumOfEvents = new ConcurrentHashMap<>(100000);
	
	Logger logger = LoggerFactory.getLogger(EventsBasketFromMongo.class);
	
	public void addEventToCollectedEvents(String uuid, BackgammonEvent backgammonEvent) {
		try{
			if(!eventsMap.containsKey(UUID.fromString(uuid))){
				Set<BackgammonEvent> eventsSet = new CopyOnWriteArraySet<>();
				eventsSet.add(backgammonEvent);
				eventsMap.put(UUID.fromString(uuid), eventsSet);
			}
			else{	
				Set<BackgammonEvent> eventsSet = eventsMap.get(UUID.fromString(uuid));
				eventsSet.add(backgammonEvent);
			}
		}
		catch(Exception ex){
			logger.error("Failed to add event to collections of events...");
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}		
	}
	
	public void putTotalNumOfEventsFor(String uuid, int tempTotalNumOfEvents){
		totalNumOfEvents.put(UUID.fromString(uuid), Integer.valueOf(tempTotalNumOfEvents));
	}
	
	public boolean isReadyHandleEventsFromMongo(String uuid){
		return (totalNumOfEvents.get(UUID.fromString(uuid)) != null && eventsMap.get(UUID.fromString(uuid)) != null && eventsMap.get(UUID.fromString(uuid)).size() == totalNumOfEvents.get(UUID.fromString(uuid)));
	}
	
	public void cleanByUuid(String uuid){
		totalNumOfEvents.remove(UUID.fromString(uuid));
		eventsMap.remove(UUID.fromString(uuid));
	}
	
	public Set<BackgammonEvent> getEvents(String uuid){
		return eventsMap.get(UUID.fromString(uuid));
	}
}
