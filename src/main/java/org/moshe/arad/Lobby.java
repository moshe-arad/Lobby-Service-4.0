package org.moshe.arad;

import java.util.HashMap;
import java.util.Map;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.stereotype.Service;

@Service
public class Lobby {

	private Map<String,BackgammonUser> users = new HashMap<>(1000);
	
	
	public void addBackgammonUserToLobby(BackgammonUser user){
		users.put(user.getUserName(), user);
	}
}
