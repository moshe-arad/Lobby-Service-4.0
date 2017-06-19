package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;
import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class GameRoomClosedEvent extends BackgammonEvent {

	private String loggedOutUserName;
	private GameRoom gameRoom;

	public GameRoomClosedEvent() {
		
	}

	public GameRoomClosedEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			String closedByUserName, GameRoom gameRoom) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.loggedOutUserName = closedByUserName;
		this.gameRoom = gameRoom;
	}
	
	public GameRoomClosedEvent(String closedByUserName, GameRoom gameRoom) {
		super();
		this.loggedOutUserName = closedByUserName;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "GameRoomClosedEvent [closedByUserName=" + loggedOutUserName + ", gameRoom=" + gameRoom + "]";
	}

	public String getLoggedOutUserName() {
		return loggedOutUserName;
	}

	public void setLoggedOutUserName(String loggedOutUserName) {
		this.loggedOutUserName = loggedOutUserName;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
