package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class OpenByLeftLastEvent extends BackgammonEvent {

	private String openBy;
	private GameRoom gameRoom;
	
	public OpenByLeftLastEvent() {
	
	}

	public OpenByLeftLastEvent(String openBy, GameRoom gameRoom) {
		super();
		this.openBy = openBy;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "OpenByLeftLastEvent [openBy=" + openBy + ", gameRoom=" + gameRoom + "]";
	}

	public String getOpenBy() {
		return openBy;
	}

	public void setOpenBy(String openBy) {
		this.openBy = openBy;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
