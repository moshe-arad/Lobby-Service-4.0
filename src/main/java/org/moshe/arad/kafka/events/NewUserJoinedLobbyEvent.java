package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;

public class NewUserJoinedLobbyEvent extends BackgammonEvent {

	private BackgammonUser backgammonUser;
	
	public NewUserJoinedLobbyEvent() {
	}

	public NewUserJoinedLobbyEvent(UUID uuid, int serviceId, String serviceName, int entityId, String entityType,
			int eventId, String eventType, Date arrived, BackgammonUser backgammonUser) {
		super(uuid, serviceId, serviceName, entityId, entityType, eventId, eventType, arrived);
		this.backgammonUser = backgammonUser;
	}

	public NewUserJoinedLobbyEvent(UUID uuid, int serviceId, String serviceName, int entityId, String entityType,
			int eventId, String eventType, BackgammonUser backgammonUser) {
		super(uuid, serviceId, serviceName, entityId, entityType, eventId, eventType);
		this.backgammonUser = backgammonUser;
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}	
}
