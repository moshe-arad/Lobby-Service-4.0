package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class ExistingUserJoinedLobbyEvent extends BackgammonEvent {

	private BackgammonUser backgammonUser;

	public ExistingUserJoinedLobbyEvent() {

	}
	
	public ExistingUserJoinedLobbyEvent(BackgammonUser backgammonUser) {
		super();
		this.backgammonUser = backgammonUser;
	}

	public ExistingUserJoinedLobbyEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			BackgammonUser backgammonUser) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.backgammonUser = backgammonUser;
	}

	@Override
	public String toString() {
		return "ExistingUserJoinedLobbyEvent [backgammonUser=" + backgammonUser + "]";
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}
}
