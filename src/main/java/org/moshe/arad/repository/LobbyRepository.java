package org.moshe.arad.repository;

import java.util.List;
import java.util.Map;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.entities.GameRoom;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class LobbyRepository {

	@Autowired
	private SnapshotAPI snapshotAPI;
	
	private Logger logger = LoggerFactory.getLogger(LobbyRepository.class);
	
	public LobbyRepository() {
	}
	
	
	public List<GameRoom> findAllGameRooms(){
		Map<String,Map<Object, Object>> snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
		
//		if(snapshot == null) return null;
//		else return isUserExistsInSnapshot(user, snapshot);
		
		return null;
	}
	
//	public boolean isUserExists(BackgammonUser user) {
//		
//		Map<String,Map<Object, Object>> snapshot = snapshotAPI.doEventsFoldingAndGetInstanceWithoutSaving();
//		
//		if(snapshot == null) return false;
//		else return isUserExistsInSnapshot(user, snapshot);				
//	}
//		
//	private boolean isUserExistsInSnapshot(BackgammonUser user, Map<String,Map<Object, Object>> snapshot){
//		if(snapshot.get(SnapshotAPI.CREATED_AND_LOGGED_IN).containsKey(user.getUserName())) return true;
//		if(snapshot.get(SnapshotAPI.LOGGED_IN).containsKey(user.getUserName())) return true;
//		if(snapshot.get(SnapshotAPI.LOBBY).containsKey(user.getUserName())) return true;
//		if(snapshot.get(SnapshotAPI.GAME).containsKey(user.getUserName())) return true;
//		if(snapshot.get(SnapshotAPI.LOGGED_OUT).containsKey(user.getUserName())) return true;
//		return false;		
//	}
}
