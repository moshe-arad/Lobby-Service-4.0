package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class UserAddedAsWatcherEvent extends BackgammonEvent {

	private String username;
	private GameRoom gameRoom;
	
	public UserAddedAsWatcherEvent() {
	
	}

	public UserAddedAsWatcherEvent(String username, GameRoom gameRoom) {
		super();
		this.username = username;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "UserAddedAsWatcherEventAck [username=" + username + ", gameRoom=" + gameRoom + "]";
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
