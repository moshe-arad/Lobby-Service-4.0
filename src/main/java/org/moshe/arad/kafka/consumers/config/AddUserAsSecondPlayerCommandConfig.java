package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.springframework.stereotype.Component;

@Component
public class AddUserAsSecondPlayerCommandConfig extends SimpleConsumerConfig{

	public AddUserAsSecondPlayerCommandConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.ADD_USER_AS_SECOND_PLAYER_COMMAND_GROUP);
	}
}
