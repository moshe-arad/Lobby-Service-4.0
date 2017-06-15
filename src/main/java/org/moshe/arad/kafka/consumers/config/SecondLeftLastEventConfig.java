package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class SecondLeftLastEventConfig extends SimpleConsumerConfig{

	public SecondLeftLastEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.SECOND_LEFT_LAST_EVENT_GROUP);
	}
}
