package org.moshe.arad.kafka.commands;

import org.springframework.stereotype.Component;

@Component
public class CloseGameRoomCommand extends Command{

	private String openedBy;

	public CloseGameRoomCommand() {
	
	}
	
	public CloseGameRoomCommand(String openedBy) {
		super();
		this.openedBy = openedBy;
	}

	@Override
	public String toString() {
		return "CloseGameRoomCommand [openedBy=" + openedBy + "]";
	}

	public String getOpenedBy() {
		return openedBy;
	}

	public void setOpenedBy(String openedBy) {
		this.openedBy = openedBy;
	}
}
