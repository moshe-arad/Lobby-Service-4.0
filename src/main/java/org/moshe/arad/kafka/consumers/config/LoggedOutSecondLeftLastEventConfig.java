package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutSecondLeftLastEventConfig extends SimpleConsumerConfig{

	public LoggedOutSecondLeftLastEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_SECOND_LEFT_LAST_EVENT_GROUP);
	}
}
