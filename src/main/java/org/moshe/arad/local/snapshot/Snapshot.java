package org.moshe.arad.local.snapshot;

import java.util.HashMap;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class Snapshot {

	private HashMap<Object, Object> rooms = new HashMap<>(100000);
	private HashMap<Object, Object> usersOpenedBy = new HashMap<>(100000);
	private HashMap<Object, Object> usersWatchers = new HashMap<>(100000);
	private HashMap<Object, Object> usersSecond = new HashMap<>(100000);
	
	public Snapshot() {
	
	}

	public Snapshot(HashMap<Object, Object> rooms, HashMap<Object, Object> usersOpenedBy,
			HashMap<Object, Object> usersWatchers) {
		super();
		this.rooms = rooms;
		this.usersOpenedBy = usersOpenedBy;
		this.usersWatchers = usersWatchers;
	}

	@Override
	public String toString() {
		return "Snapshot [rooms=" + rooms + ", usersOpenedBy=" + usersOpenedBy + ", usersWatchers=" + usersWatchers
				+ "]";
	}

	public HashMap<Object, Object> getRooms() {
		return rooms;
	}

	public void setRooms(HashMap<Object, Object> rooms) {
		this.rooms = rooms;
	}

	public HashMap<Object, Object> getUsersOpenedBy() {
		return usersOpenedBy;
	}

	public void setUsersOpenedBy(HashMap<Object, Object> usersOpenedBy) {
		this.usersOpenedBy = usersOpenedBy;
	}

	public HashMap<Object, Object> getUsersWatchers() {
		return usersWatchers;
	}

	public void setUsersWatchers(HashMap<Object, Object> usersWatchers) {
		this.usersWatchers = usersWatchers;
	}

	public HashMap<Object, Object> getUsersSecond() {
		return usersSecond;
	}

	public void setUsersSecond(HashMap<Object, Object> usersSecond) {
		this.usersSecond = usersSecond;
	}
}
