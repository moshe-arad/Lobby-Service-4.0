package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class LoggedOutWatcherLeftLastEvent extends BackgammonEvent {

	private String watcher;
	private GameRoom gameRoom;
	
	public LoggedOutWatcherLeftLastEvent() {
	
	}

	public LoggedOutWatcherLeftLastEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			String watcher, GameRoom gameRoom) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.watcher = watcher;
		this.gameRoom = gameRoom;
	}

	public LoggedOutWatcherLeftLastEvent(String watcher, GameRoom gameRoom) {
		super();
		this.watcher = watcher;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "LoggedOutWatcherLeftLastEvent [watcher=" + watcher + ", gameRoom=" + gameRoom + "]";
	}

	public String getWatcher() {
		return watcher;
	}

	public void setWatcher(String watcher) {
		this.watcher = watcher;
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}
}
