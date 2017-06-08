package org.moshe.arad.kafka.consumers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class LoggedOutWatcherLeftLastEventConfig extends SimpleConsumerConfig{

	public LoggedOutWatcherLeftLastEventConfig() {
		super();
		super.getProperties().put("group.id", KafkaUtils.LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_GROUP);
	}
}
