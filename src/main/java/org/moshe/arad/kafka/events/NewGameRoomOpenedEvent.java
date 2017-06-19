package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class NewGameRoomOpenedEvent extends BackgammonEvent {

	private GameRoom gameRoom;
	
	public NewGameRoomOpenedEvent() {
	
	}

	public NewGameRoomOpenedEvent(GameRoom gameRoom) {
		super();
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "NewGameRoomOpenedEvent [gameRoom=" + gameRoom + "]";
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}	
}
