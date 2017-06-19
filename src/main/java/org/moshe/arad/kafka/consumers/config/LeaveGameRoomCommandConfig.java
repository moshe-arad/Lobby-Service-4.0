package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LeaveGameRoomCommandConfig extends SimpleConsumerConfig{

	public LeaveGameRoomCommandConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LEAVE_GAME_ROOM_COMMAND_GROUP);
	}
}
