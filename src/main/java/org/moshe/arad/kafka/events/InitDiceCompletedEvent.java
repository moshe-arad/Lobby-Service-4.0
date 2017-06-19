package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class InitDiceCompletedEvent extends BackgammonEvent{

	private String username;
	private String gameRoomName;
	
	public InitDiceCompletedEvent() {
	
	}

	public InitDiceCompletedEvent(String username, String gameRoomName) {
		super();
		this.username = username;
		this.gameRoomName = gameRoomName;
	}

	@Override
	public String toString() {
		return "InitDiceCompletedEvent [username=" + username + ", gameRoomName=" + gameRoomName + "]";
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getGameRoomName() {
		return gameRoomName;
	}

	public void setGameRoomName(String gameRoomName) {
		this.gameRoomName = gameRoomName;
	}
}
