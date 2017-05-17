package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class CloseGameRoomEventAck extends BackgammonEvent{

	private boolean isGameRoomClosed;
	private GameRoom gameRoom;
	
	public CloseGameRoomEventAck() {
	
	}

	public CloseGameRoomEventAck(boolean isGameRoomClosed, GameRoom gameRoom) {
		super();
		this.isGameRoomClosed = isGameRoomClosed;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "CloseGameRoomEventAck [isGameRoomClosed=" + isGameRoomClosed + ", gameRoom=" + gameRoom + "]";
	}

	public boolean isGameRoomClosed() {
		return isGameRoomClosed;
	}

	public void setGameRoomClosed(boolean isGameRoomClosed) {
		this.isGameRoomClosed = isGameRoomClosed;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
