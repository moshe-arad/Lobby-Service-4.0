package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutOpenByLeftLastEventConfig extends SimpleConsumerConfig{

	public LoggedOutOpenByLeftLastEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_GROUP);
	}
}
