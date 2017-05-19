package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class UserAddedAsWatcherEventAck extends BackgammonEvent {

	private boolean isUserAddedAsWatcher;
	private String username;
	private GameRoom gameRoom;
	
	public UserAddedAsWatcherEventAck() {
	
	}

	public UserAddedAsWatcherEventAck(String username, GameRoom gameRoom) {
		super();
		this.username = username;
		this.gameRoom = gameRoom;
	}
	
	public UserAddedAsWatcherEventAck(boolean isUserAddedAsWatcher, String username, GameRoom gameRoom) {
		super();
		this.isUserAddedAsWatcher = isUserAddedAsWatcher;
		this.username = username;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "UserAddedAsWatcherEventAck [isUserAddedAsWatcher=" + isUserAddedAsWatcher + ", username=" + username
				+ ", gameRoom=" + gameRoom + "]";
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

	public boolean isUserAddedAsWatcher() {
		return isUserAddedAsWatcher;
	}

	public void setUserAddedAsWatcher(boolean isUserAddedAsWatcher) {
		this.isUserAddedAsWatcher = isUserAddedAsWatcher;
	}
}
