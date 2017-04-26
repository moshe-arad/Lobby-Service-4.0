package org.moshe.arad.kafka;

public class KafkaUtils {

	public static final String SERVERS = "192.168.1.4:9092,192.168.1.4:9093,192.168.1.4:9094";
	public static final String CREATE_NEW_USER_COMMAND_GROUP = "CreateNewUserCommandGroup";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String CREATE_NEW_USER_COMMAND_DESERIALIZER = "org.moshe.arad.kafka.deserializers.CreateNewUserCommandDeserializer";
	public static final String NEW_USER_CREATED_EVENT_SERIALIZER = "org.moshe.arad.kafka.serializers.NewUserCreatedEventSerializer";
	public static final String COMMANDS_TO_USERS_SERVICE_TOPIC = "Commands-To-Users-Service";
	public static final String NEW_USER_CREATED_EVENT_DESERIALIZER = "org.moshe.arad.kafka.deserializers.NewUserCreatedEventDeserializer";
	public static final String NEW_USER_CREATED_EVENT_TOPIC = "New-User-Created-Event";
	public static final String NEW_USER_JOINED_LOBBY_EVENT_TOPIC = "New-User-Joined-Lobby-Event";
	public static final String NEW_USER_JOINED_LOBBY_EVENT_SERIALIZER = "org.moshe.arad.kafka.serializers.NewUserJoinedLobbyEventSerializer";
	public static final String NEW_USER_CREATED_EVENT_GROUP = "NewUserCreatedEventGroup2";
}
