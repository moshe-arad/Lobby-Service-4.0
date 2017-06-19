package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

public abstract class BackgammonEvent {

	private UUID uuid;
	private int serviceId;
	private int eventId;
	private Date arrived;
	private String clazz;
	
	public BackgammonEvent() {
	}
	
	public BackgammonEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz) {
		super();
		this.uuid = uuid;
		this.serviceId = serviceId;
		this.eventId = eventId;
		this.arrived = arrived;
		this.clazz = clazz;
	}

	
	@Override
	public String toString() {
		return "BackgammonEvent [uuid=" + uuid + ", serviceId=" + serviceId + ", eventId=" + eventId + ", arrived="
				+ arrived + ", clazz=" + clazz + "]";
	}

	public int getServiceId() {
		return serviceId;
	}

	public void setServiceId(int serviceId) {
		this.serviceId = serviceId;
	}

	public int getEventId() {
		return eventId;
	}

	public void setEventId(int eventId) {
		this.eventId = eventId;
	}

	public Date getArrived() {
		return arrived;
	}

	public void setArrived(Date arrived) {
		this.arrived = arrived;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}	
}
