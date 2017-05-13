package org.moshe.arad.kafka.commands;

import java.util.UUID;

public abstract class Command implements ICommand{

	private UUID uuid;

	public Command() {
	}
	
	public Command(UUID uuid) {
		super();
		this.uuid = uuid;
	}

	@Override
	public String toString() {
		return "Command [uuid=" + uuid + "]";
	}

	@Override
	public UUID getUuid() {
		return uuid;
	}
	
	@Override
	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
}
