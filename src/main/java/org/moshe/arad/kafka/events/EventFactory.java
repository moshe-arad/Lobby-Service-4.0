package org.moshe.arad.kafka.events;

import java.util.Date;

import org.moshe.arad.entities.BackgammonUser;

public class EventFactory {
	
	public static BackgammonEvent getEvent(Events event, BackgammonUser backgammonUser, Date arrived){
		if(event.equals(Events.NewUserCreatedEvent)){
			return new NewUserCreatedEvent(1, "UsersService", 1, "Users", 1000, Events.NewUserCreatedEvent.name(), backgammonUser);
		}
		else if(event.equals(Events.NewUserJoinedLobbyEvent)){
			return new NewUserJoinedLobbyEvent(2, "Lobby Service", 1, "Users", 2, Events.NewUserJoinedLobbyEvent.name(), backgammonUser);
		}
		else if(event.equals(Events.NewUserCreatedEventWithSameDate)){
			return new NewUserCreatedEvent(1, "UsersService", 1, "Users", 1, Events.NewUserCreatedEvent.name(), backgammonUser, arrived);
		}
		else return null;
	}
}
