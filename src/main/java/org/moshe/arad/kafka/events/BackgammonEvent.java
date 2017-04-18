package org.moshe.arad.kafka.events;

import java.util.Date;

public abstract class BackgammonEvent {

	private int serviceId;
	private String serviceName;
	private int entityId;
	private String entityType;
	private int eventId;
	private String eventType;
	private Date arrived;
	private Date departed;
	
	public BackgammonEvent() {
	}

	public BackgammonEvent(int serviceId, String serviceName, int entityId, String entityType, int eventId,
			String eventType) {
		this.serviceId = serviceId;
		this.serviceName = serviceName;
		this.entityId = entityId;
		this.entityType = entityType;
		this.eventId = eventId;
		this.eventType = eventType;
		this.arrived = new Date();
	}

	public BackgammonEvent(int serviceId, String serviceName, int entityId, String entityType, int eventId,
			String eventType, Date arrived) {
		super();
		this.serviceId = serviceId;
		this.serviceName = serviceName;
		this.entityId = entityId;
		this.entityType = entityType;
		this.eventId = eventId;
		this.eventType = eventType;
		this.arrived = arrived;
	}

	@Override
	public String toString() {
		return "BackgammonEvent [serviceId=" + serviceId + ", serviceName=" + serviceName + ", entityId=" + entityId
				+ ", entityType=" + entityType + ", eventId=" + eventId + ", eventType=" + eventType + ", arrived="
				+ arrived + ", departed=" + departed + "]";
	}

	public int getServiceId() {
		return serviceId;
	}

	public void setServiceId(int serviceId) {
		this.serviceId = serviceId;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public int getEntityId() {
		return entityId;
	}

	public void setEntityId(int entityId) {
		this.entityId = entityId;
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public int getEventId() {
		return eventId;
	}

	public void setEventId(int eventId) {
		this.eventId = eventId;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public Date getArrived() {
		return arrived;
	}

	public void setArrived(Date arrived) {
		this.arrived = arrived;
	}

	public Date getDeparted() {
		return departed;
	}

	public void setDeparted(Date departed) {
		this.departed = departed;
	}	
}
