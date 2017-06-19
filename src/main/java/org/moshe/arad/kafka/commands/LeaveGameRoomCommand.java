package org.moshe.arad.kafka.commands;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class LeaveGameRoomCommand extends Command {

	private String username;
	private String gameRoomName;
	
	public LeaveGameRoomCommand() {
	
	}

	public LeaveGameRoomCommand(String username, String gameRoomName) {
		super();
		this.username = username;
		this.gameRoomName = gameRoomName;
	}

	@Override
	public String toString() {
		return "LeaveGameRoomCommand [username=" + username + ", gameRoomName=" + gameRoomName + "]";
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getGameRoomName() {
		return gameRoomName;
	}

	public void setGameRoomName(String gameRoomName) {
		this.gameRoomName = gameRoomName;
	}
}
