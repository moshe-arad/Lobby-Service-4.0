package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class OpenByLeftLastEventConfig extends SimpleConsumerConfig{

	public OpenByLeftLastEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.OPENBY_LEFT_LAST_EVENT_GROUP);
	}
}
