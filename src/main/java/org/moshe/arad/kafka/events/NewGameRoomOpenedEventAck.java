package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class NewGameRoomOpenedEventAck extends BackgammonEvent{

	private boolean isGameRoomOpened;
	private GameRoom gameRoom;
	
	public NewGameRoomOpenedEventAck() {
	
	}
	
	public NewGameRoomOpenedEventAck(boolean isGameRoomOpened, GameRoom gameRoom) {
		super();
		this.isGameRoomOpened = isGameRoomOpened;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "NewGameRoomOpenedEventAck [isGameRoomOpened=" + isGameRoomOpened + ", gameRoom=" + gameRoom + "]";
	}

	public boolean isGameRoomOpened() {
		return isGameRoomOpened;
	}

	public void setGameRoomOpened(boolean isGameRoomOpened) {
		this.isGameRoomOpened = isGameRoomOpened;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
