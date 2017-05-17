package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class GameRoomClosedEvent extends BackgammonEvent{

	private GameRoom gameRoom;
	
	public GameRoomClosedEvent() {
	
	}

	public GameRoomClosedEvent(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "GameRoomClosedEvent [gameRoom=" + gameRoom + "]";
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
