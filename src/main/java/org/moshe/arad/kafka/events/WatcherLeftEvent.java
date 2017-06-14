package org.moshe.arad.kafka.events;

import org.moshe.arad.entities.GameRoom;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class WatcherLeftEvent extends BackgammonEvent {

	private String watcher;
	private GameRoom gameRoom;
	
	public WatcherLeftEvent() {
	
	}
	
	public WatcherLeftEvent(String watcher, GameRoom gameRoom) {
		super();
		this.watcher = watcher;
		this.gameRoom = gameRoom;
	}

	@Override
	public String toString() {
		return "WatcherLeftEvent [watcher=" + watcher + ", gameRoom=" + gameRoom + "]";
	}

	public GameRoom getGameRoom() {
		return gameRoom;
	}

	public void setGameRoom(GameRoom gameRoom) {
		this.gameRoom = gameRoom;
	}

	public String getWatcher() {
		return watcher;
	}

	public void setWatcher(String watcher) {
		this.watcher = watcher;
	}
}
