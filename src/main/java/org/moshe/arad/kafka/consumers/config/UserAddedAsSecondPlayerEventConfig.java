package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class UserAddedAsSecondPlayerEventConfig extends SimpleConsumerConfig{

	public UserAddedAsSecondPlayerEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.USER_ADDED_AS_SECOND_PLAYER_EVENT_GROUP);
	}
}
