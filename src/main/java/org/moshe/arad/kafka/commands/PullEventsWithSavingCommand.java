package org.moshe.arad.kafka.commands;

import java.util.Date;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public class PullEventsWithSavingCommand implements ICommand {

	private UUID uuid;
	private Date fromDate;
	private boolean isIgnoreDate;
	
	public PullEventsWithSavingCommand() {
	
	}

	public PullEventsWithSavingCommand(UUID uuid, Date fromDate, boolean isIgnoreDate) {
		super();
		this.uuid = uuid;
		this.fromDate = fromDate;
		this.isIgnoreDate = isIgnoreDate;
	}

	@Override
	public String toString() {
		return "PullEventsWithSavingCommand [uuid=" + uuid + ", fromDate=" + fromDate + ", isIgnoreDate=" + isIgnoreDate
				+ "]";
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	public Date getFromDate() {
		return fromDate;
	}

	public void setFromDate(Date fromDate) {
		this.fromDate = fromDate;
	}

	public boolean isIgnoreDate() {
		return isIgnoreDate;
	}

	public void setIgnoreDate(boolean isIgnoreDate) {
		this.isIgnoreDate = isIgnoreDate;
	}
}
