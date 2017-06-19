package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class OpenByLeftBeforeGameStartedEventConfig extends SimpleConsumerConfig{

	public OpenByLeftBeforeGameStartedEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_GROUP);
	}
}
