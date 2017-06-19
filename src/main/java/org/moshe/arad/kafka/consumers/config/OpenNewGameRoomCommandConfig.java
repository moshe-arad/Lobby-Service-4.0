package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class OpenNewGameRoomCommandConfig extends SimpleConsumerConfig{

	public OpenNewGameRoomCommandConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.OPEN_NEW_GAME_ROOM_COMMAND_GROUP);
	}
}
