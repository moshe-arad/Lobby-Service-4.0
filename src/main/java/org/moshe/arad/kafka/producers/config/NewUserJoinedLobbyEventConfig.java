package org.moshe.arad.kafka.producers.config;

import org.moshe.arad.kafka.KafkaUtils;
import org.springframework.stereotype.Component;

@Component
public class NewUserJoinedLobbyEventConfig extends SimpleProducerConfig {

	public NewUserJoinedLobbyEventConfig() {
		super();
		super.getProperties().put("value.serializer", KafkaUtils.NEW_USER_JOINED_LOBBY_EVENT_SERIALIZER);
	}
}
