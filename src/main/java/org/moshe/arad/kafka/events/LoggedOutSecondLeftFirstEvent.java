package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class LoggedOutSecondLeftFirstEvent extends BackgammonEvent {

	private String second;
	private GameRoom gameRoom;
	
	public LoggedOutSecondLeftFirstEvent() {
	
	}

	public LoggedOutSecondLeftFirstEvent(String second, GameRoom gameRoom) {
		super();
		this.second = second;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "LoggedOutSecondLeftFirstEvent [second=" + second + ", gameRoom=" + gameRoom + "]";
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}
}
