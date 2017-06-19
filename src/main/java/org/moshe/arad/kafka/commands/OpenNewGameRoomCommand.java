package org.moshe.arad.kafka.commands;

import org.springframework.stereotype.Component;

@Component
public class OpenNewGameRoomCommand extends Command{

	private String username;

	public OpenNewGameRoomCommand() {
	
	}
	
	public OpenNewGameRoomCommand(String username) {
		super();
		this.username = username;
	}

	@Override
	public String toString() {
		return "OpenNewGameRoomCommand [username=" + username + "]";
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
}
