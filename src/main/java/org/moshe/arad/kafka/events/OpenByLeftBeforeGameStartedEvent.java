package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class OpenByLeftBeforeGameStartedEvent extends BackgammonEvent {

	private String leavingUserName;
	private GameRoom gameRoom;

	public OpenByLeftBeforeGameStartedEvent() {
	}

	public OpenByLeftBeforeGameStartedEvent(String leavingUserName, GameRoom gameRoom) {
		super();
		this.leavingUserName = leavingUserName;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "OpenByLeftBeforeGameStartedEvent [leavingUserName=" + leavingUserName + ", gameRoom=" + gameRoom + "]";
	}

	public String getLeavingUserName() {
		return leavingUserName;
	}

	public void setLeavingUserName(String leavingUserName) {
		this.leavingUserName = leavingUserName;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
