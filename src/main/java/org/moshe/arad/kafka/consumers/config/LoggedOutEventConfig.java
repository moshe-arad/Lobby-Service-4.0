package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutEventConfig extends SimpleConsumerConfig{

	public LoggedOutEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_EVENT_GROUP);
	}
}
