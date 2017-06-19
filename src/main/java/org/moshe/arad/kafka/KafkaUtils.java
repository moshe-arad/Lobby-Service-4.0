package org.moshe.arad.kafka;

public class KafkaUtils {

	public static final String SERVERS = "localhost:9092,localhost:9093,localhost:9094";
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
	public static final String NEW_GAME_ROOM_OPENED_EVENT_ACK_TOPIC = "New-Game-Room-Opened-Event-Ack";
	public static final String LOGGED_IN_EVENT_ACK_TOPIC = "Logged-In-Event-Ack";
	public static final String CLOSE_GAME_ROOM_COMMAND_GROUP = "CloseGameRoomCommandGroup";
	public static final String CLOSE_GAME_ROOM_COMMAND_TOPIC = "Close-Game-Room-Command";
	public static final String CLOSE_GAME_ROOM_EVENT_ACK_TOPIC = "Close-Game-Room-Event-Ack";
	public static final String GAME_ROOM_CLOSED_EVENT_TOPIC = "Game-Room-Closed-Event";
	public static final String ADD_USER_AS_WATCHER_COMMAND_GROUP = "AddUserAsWatcherCommandGroup";
	public static final String ADD_USER_AS_WATCHER_COMMAND_TOPIC = "Add-User-As-Watcher-Command";
	public static final String USER_ADDED_AS_WATCHER_EVENT_TOPIC = "User-Added-As-Watcher-Event";
	public static final String USER_ADDED_AS_WATCHER_EVENT_ACK_TOPIC = "User-Added-As-Watcher-Event-Ack";
	public static final String LOGGED_OUT_EVENT_GROUP = "LoggedOutEventGroup2";
	public static final String LOGGED_OUT_EVENT_TOPIC = "Logged-Out-Event";
	public static final String GAME_ROOM_CLOSED_EVENT_LOGOUT_TOPIC = "Game-Room-Closed-Event-Logout";
	public static final String WATCHER_REMOVED_EVENT_TOPIC = "Watcher-Removed-Event";
	public static final String ADD_USER_AS_SECOND_PLAYER_COMMAND_GROUP = "AddUserAsSecondPlayerCommandGroup";
	public static final String ADD_USER_AS_SECOND_PLAYER_COMMAND_TOPIC = "Add-User-As-Second-Player-Command";
	public static final String USER_ADDED_AS_SECOND_PLAYER_EVENT_TOPIC = "User-Added-As-Second-Player-Event";
	public static final String LOGGED_OUT_USER_LEFT_LOBBY_EVENT_TOPIC = "Logged-Out-User-Left-Lobby-Event";
	public static final String LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC = "Logged-Out-Openby-Left-Before-Game-Started-Event";
	public static final String LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_GROUP = "LoggedOutOpenByLeftBeforeGameStartedEventGroup3";
	public static final String GAME_ROOM_CLOSED_LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC = "Game-Room-Closed-Logged-Out-Openby-Left-Before-Game-Started-Event";
	public static final String LOGGED_OUT_OPENBY_LEFT_EVENT_TOPIC = "Logged-Out-Openby-Left-Event";
	public static final String LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC = "Logged-Out-Watcher-Left-Last-Event";
	public static final String LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_GROUP = "LoggedOutWatcherLeftLastEventGroup1";
	public static final String GAME_ROOM_CLOSED_LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC = "Game-Room-Closed-Logged-Out-Watcher-Left-Last-Event";
	public static final String LOGGED_OUT_WATCHER_LEFT_EVENT_TOPIC = "Logged-Out-Watcher-Left-Event";
	public static final String LOGGED_OUT_OPENBY_LEFT_FIRST_EVENT_TOPIC = "Logged-Out-Openby-Left-First-Event";
	public static final String LOGGED_OUT_SECOND_LEFT_FIRST_EVENT_TOPIC = "Logged-Out-Second-Left-First-Event";
	public static final String LOGGED_OUT_SECOND_LEFT_EVENT_TOPIC = "Logged-Out-Second-Left-Event";
	public static final String LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC = "Logged-Out-Openby-Left-Last-Event";
	public static final String LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_GROUP = "LoggedOutOpenbyLeftLastEventGroup3";
	public static final String GAME_ROOM_CLOSED_LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC = "Game-Room-Closed-Logged-Out-Openby-Left-Last-Event";
	public static final String LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC = "Logged-Out-Second-Left-Last-Event";
	public static final String LOGGED_OUT_SECOND_LEFT_LAST_EVENT_GROUP = "LoggedOutSecondLeftLastEventGroup3";
	public static final String GAME_ROOM_CLOSED_LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC = "Game-Room-Closed-Logged-Out-Second-Left-Last-Event";
	public static final String LEAVE_GAME_ROOM_COMMAND_GROUP = "LeaveGameRoomCommandGroup1";
	public static final String LEAVE_GAME_ROOM_COMMAND_TOPIC = "Leave-Game-Room-Command";
	public static final String OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC = "Openby-Left-Before-Game-Started-Event";
	public static final String OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_GROUP = "OpenbyLeftBeforeGameStartedEventGroup4";
	public static final String GAME_ROOM_CLOSED_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC = "Game-Room-Closed-Openby-Left-Before-Game-Started-Event";
	public static final String OPENBY_LEFT_EVENT_TOPIC = "Openby-Left-Event";
	public static final String WATCHER_LEFT_LAST_EVENT_TOPIC = "Watcher-Left-Last-Event";
	public static final String WATCHER_LEFT_LAST_EVENT_GROUP = "WatcherLeftLastEventGroup3";
	public static final String GAME_ROOM_CLOSED_WATCHER_LEFT_LAST_EVENT_TOPIC = "Game-Room-Closed-Watcher-Left-Last-Event";
	public static final String WATCHER_LEFT_EVENT_TOPIC = "Watcher-Left-Event";
	public static final String OPENBY_LEFT_FIRST_EVENT_TOPIC = "Openby-Left-First-Event";
	public static final String SECOND_LEFT_FIRST_EVENT_TOPIC = "Second-Left-First-Event";
	public static final String SECOND_LEFT_EVENT_TOPIC = "Second-Left-Event";
	public static final String OPENBY_LEFT_LAST_EVENT_TOPIC = "Openby-Left-Last-Event";
	public static final String OPENBY_LEFT_LAST_EVENT_GROUP = "OpenbyLeftLastEventGroup3";
	public static final String GAME_ROOM_CLOSED_OPENBY_LEFT_LAST_EVENT_TOPIC = "Game-Room-Closed-Openby-Left-Last-Event";
	public static final String SECOND_LEFT_LAST_EVENT_TOPIC = "Second-Left-Last-Event";
	public static final String SECOND_LEFT_LAST_EVENT_GROUP = "SecondLeftLastEventGroup3";
	public static final String GAME_ROOM_CLOSED_SECOND_LEFT_LAST_EVENT_TOPIC = "Game-Room-Closed-Second-Left-Last-Event";
	public static final Object USER_ADDED_AS_SECOND_PLAYER_EVENT_GROUP = "UserAddedAsSecondPlayerEventGroup4";
	public static final String INIT_GAME_ROOM_COMPLETED_EVENT_TOPIC = "Init-Game-Room-Completed-Event";
	public static final String INIT_DICE_COMPLETED_EVENT_GROUP = "InitDiceCompletedEventGroup2";
	public static final String INIT_DICE_COMPLETED_EVENT_TOPIC = "Init-Dice-Completed-Event";
	public static final String ROLL_DICE_GAME_ROOM_FOUND_EVENT_TOPIC = "Roll-Dice-Game-Room-Found-Event";
}
