package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.springframework.stereotype.Component;

@Component
public class CloseGameRoomCommandConfig extends SimpleConsumerConfig{

	public CloseGameRoomCommandConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.CLOSE_GAME_ROOM_COMMAND_GROUP);
	}
}
