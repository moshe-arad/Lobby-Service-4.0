package org.moshe.arad.entities;

import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class GameRoom {

	private String name;
	private String openBy;
	private String secondPlayer;
	private List<String> watchers;
	
	public GameRoom() {
	
	}

	public GameRoom(String name, String openBy, String secondPlayer, List<String> watchers) {
		super();
		this.name = name;
		this.openBy = openBy;
		this.secondPlayer = secondPlayer;
		this.watchers = watchers;
	}

	@Override
	public String toString() {
		return "GameRoom [name=" + name + ", openBy=" + openBy + ", secondPlayer=" + secondPlayer + ", watchers="
				+ watchers + "]";
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOpenBy() {
		return openBy;
	}

	public void setOpenBy(String openBy) {
		this.openBy = openBy;
	}

	public String getSecondPlayer() {
		return secondPlayer;
	}

	public void setSecondPlayer(String secondPlayer) {
		this.secondPlayer = secondPlayer;
	}

	public List<String> getWatchers() {
		return watchers;
	}

	public void setWatchers(List<String> watchers) {
		this.watchers = watchers;
	}
}
