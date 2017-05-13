package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class FromMongoWithoutSavingEventsConfig extends SimpleConsumerConfig {

	public FromMongoWithoutSavingEventsConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.TO_LOBBY_FROM_MONGO_EVENTS_WITHOUT_SAVING_GROUP);
	}
}
