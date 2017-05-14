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
	public static final String LOGGED_IN_EVENT_GROUP = "LoggedInEventGroup2";
	public static final String LOGGED_IN_EVENT_TOPIC = "Logged-In-Event";
	public static final String EXISTING_USER_JOINED_LOBBY_EVENT_TOPIC = "Existing-User-Joined-Lobby-Event";
	public static final String NEW_USER_CREATED_EVENT_ACK_TOPIC = "New-User-Created-Event-Ack";
	public static final String LOBBY_SERVICE_PULL_EVENTS_WITH_SAVING_COMMAND_TOPIC = "Lobby-Service-Pull-Events-With-Saving-Command";
	public static final String LOBBY_SERVICE_PULL_EVENTS_WITHOUT_SAVING_COMMAND_TOPIC = "Lobby-Service-Pull-Events-Without-Saving-Command";
	public static final String TO_LOBBY_FROM_MONGO_EVENTS_WITHOUT_SAVING_TOPIC = "To-Lobby-From-Mongo-Events-Without-Saving";
	public static final String TO_LOBBY_FROM_MONGO_EVENTS_WITH_SAVING_GROUP = "ToLobbyFromMongoEventsWithSavingGroup";
	public static final String TO_LOBBY_FROM_MONGO_EVENTS_WITHOUT_SAVING_GROUP = "ToLobbyFromMongoEventsWithoutSavingGroup";
	public static final String OPEN_NEW_GAME_ROOM_COMMAND_GROUP = "OpenNewGameRoomCommandGroup";
	public static final String OPEN_NEW_GAME_ROOM_COMMAND_TOPIC = "Open-New-Game-Room-Command";
	public static final String NEW_GAME_ROOM_OPENED_EVENT_TOPIC = "New-Game-Room-Opened-Event";
}
