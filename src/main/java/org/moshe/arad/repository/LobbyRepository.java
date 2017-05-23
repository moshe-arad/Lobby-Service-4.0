package org.moshe.arad.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.local.snapshot.Snapshot;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;

@Repository
public class LobbyRepository {

	@Autowired
	private SnapshotAPI snapshotAPI;
	
	private Logger logger = LoggerFactory.getLogger(LobbyRepository.class);
	
	public LobbyRepository() {
	}
	
	public List<GameRoom> getUserEngagedGameRooms(String username){
		List<GameRoom> result = new ArrayList<>(100000);
		Snapshot snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
		if(snapshot == null) throw new RuntimeException("Failed to grab snapshot from events store...");
		else{
			if(snapshot.getUsersWatchers().containsKey(username)){
				String gameRoomName = snapshot.getUsersWatchers().get(username).toString();
				ObjectMapper objectMapper = new ObjectMapper();
				GameRoom gameRoom = null;
				try {
					gameRoom = objectMapper.readValue(snapshot.getRooms().get(gameRoomName).toString(), GameRoom.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
				result.add(gameRoom);
			}
			else if(snapshot.getUsersOpenedBy().containsKey(username)){
				String gameRoomName = snapshot.getUsersOpenedBy().get(username).toString();
				ObjectMapper objectMapper = new ObjectMapper();
				GameRoom gameRoom = null;
				try {
					gameRoom = objectMapper.readValue(snapshot.getRooms().get(gameRoomName).toString(), GameRoom.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
				result.add(gameRoom);
			}
			//TODO handle 2nd player (join)
			return result;
		}
	}
	
	public boolean isUserEngagedInOtherRoom(String username){
		Snapshot snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
		if(snapshot == null) throw new RuntimeException("Failed to grab snapshot from events store...");
		else{
			Set<Map.Entry<Object, Object>> entries = snapshot.getRooms().entrySet();
			Iterator<Map.Entry<Object, Object>> it = entries.iterator();
			
			while(it.hasNext()){
				Map.Entry<Object, Object> entry = it.next();
				ObjectMapper objectMapper = new ObjectMapper();
				try {
					GameRoom gameRoom = objectMapper.readValue(entry.getValue().toString(), GameRoom.class);
					if(gameRoom.getOpenBy().equals(username) 
							|| gameRoom.getSecondPlayer().equals(username) 
							|| gameRoom.getWatchers().contains(username)) return true;
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
		}
		return false;
	}
	
	public GameRoom getGameRoomToClose(String username){
		Snapshot snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
		if(snapshot == null) throw new RuntimeException("Failed to grab snapshot from events store...");
		else{
			if(!snapshot.getUsersOpenedBy().containsKey(username)) return null;
			else {
				ObjectMapper objectMapper = new ObjectMapper();
				GameRoom result = null;
				try {
					result = objectMapper.readValue(snapshot.getRooms().get(snapshot.getUsersOpenedBy().get(username)).toString(), GameRoom.class);
				} catch (IOException e) {
					e.printStackTrace();
				}				
				return result;
			}
		}
	}
	
	public GameRoom getGameRoomToAddWatcherTo(String gameRoomName, String watcher){
		Snapshot snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
		if(snapshot == null) throw new RuntimeException("Failed to grab snapshot from events store...");
		else if(isUserEngagedInOtherRoom(watcher, snapshot) == true) return null;
		else{
			if(!snapshot.getRooms().containsKey(gameRoomName)) return null;
			else {
				ObjectMapper objectMapper = new ObjectMapper();
				GameRoom result = null;
				try {
					result = objectMapper.readValue(snapshot.getRooms().get(gameRoomName).toString(), GameRoom.class);
				} catch (IOException e) {
					e.printStackTrace();
				}				
				return result;
			}
		}
	}
	
	private boolean isUserEngagedInOtherRoom(String username, Snapshot snapshot){
		if(snapshot == null) throw new RuntimeException("Failed to grab snapshot from events store...");
		else{
			Set<Map.Entry<Object, Object>> entries = snapshot.getRooms().entrySet();
			Iterator<Map.Entry<Object, Object>> it = entries.iterator();
			
			while(it.hasNext()){
				Map.Entry<Object, Object> entry = it.next();
				ObjectMapper objectMapper = new ObjectMapper();
				try {
					GameRoom gameRoom = objectMapper.readValue(entry.getValue().toString(), GameRoom.class);
					if(gameRoom.getOpenBy().equals(username) 
							|| gameRoom.getSecondPlayer().equals(username) 
							|| gameRoom.getWatchers().contains(username)) return true;
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
		}
		return false;
	}
}
