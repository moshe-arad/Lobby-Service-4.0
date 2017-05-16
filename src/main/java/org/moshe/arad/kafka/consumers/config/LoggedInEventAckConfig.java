package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedInEventAckConfig extends SimpleConsumerConfig{

	public LoggedInEventAckConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LOGGED_IN_EVENT_GROUP);
	}
}
