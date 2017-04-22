package org.moshe.arad;

import java.util.HashMap;
import java.util.Map;

import org.moshe.arad.entities.BackgammonUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class Lobby {

	private Map<String,BackgammonUser> users = new HashMap<>(1000);
	
	
	private Logger logger = LoggerFactory.getLogger(Lobby.class);
	
	
	public void addBackgammonUserToLobby(BackgammonUser user){
		users.put(user.getUserName(), user);
	}
}
