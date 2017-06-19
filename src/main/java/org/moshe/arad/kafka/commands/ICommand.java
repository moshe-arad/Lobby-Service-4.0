package org.moshe.arad.kafka.commands;

import java.util.UUID;

public interface ICommand {

	public void setUuid(UUID uuid);
	
	public UUID getUuid();
}
