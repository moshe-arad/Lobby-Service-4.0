package org.moshe.arad.kafka.events;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.entities.BackgammonUser;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class LoggedInEvent extends BackgammonEvent {

	private BackgammonUser backgammonUser;

	public LoggedInEvent() {
		
	}
	
	public LoggedInEvent(BackgammonUser backgammonUser) {
		super();
		this.backgammonUser = backgammonUser;
	}

	public LoggedInEvent(UUID uuid, int serviceId, int eventId, Date arrived, String clazz,
			BackgammonUser backgammonUser) {
		super(uuid, serviceId, eventId, arrived, clazz);
		this.backgammonUser = backgammonUser;
	}

	@Override
	public String toString() {
		return "LoggedInEvent [backgammonUser=" + backgammonUser + "]";
	}

	public BackgammonUser getBackgammonUser() {
		return backgammonUser;
	}

	public void setBackgammonUser(BackgammonUser backgammonUser) {
		this.backgammonUser = backgammonUser;
	}
}
