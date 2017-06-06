package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutOpenByLeftBeforeGameStartedEventConfig extends SimpleConsumerConfig{

	public LoggedOutOpenByLeftBeforeGameStartedEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_GROUP);
	}
}
