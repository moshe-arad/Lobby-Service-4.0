package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class InitDiceCompletedEventConfig extends SimpleConsumerConfig{

	public InitDiceCompletedEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.INIT_DICE_COMPLETED_EVENT_GROUP);
	}
}
