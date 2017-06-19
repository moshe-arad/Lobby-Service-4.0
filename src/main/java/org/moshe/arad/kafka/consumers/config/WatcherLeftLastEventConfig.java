package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class WatcherLeftLastEventConfig extends SimpleConsumerConfig{

	public WatcherLeftLastEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.WATCHER_LEFT_LAST_EVENT_GROUP);
	}
}
