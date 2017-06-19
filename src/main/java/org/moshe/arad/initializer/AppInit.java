package org.moshe.arad.initializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.commands.PullEventsWithSavingCommand;
import org.moshe.arad.kafka.consumers.ISimpleConsumer;
import org.moshe.arad.kafka.consumers.commands.AddUserAsSecondPlayerCommandConsumer;
import org.moshe.arad.kafka.consumers.commands.AddUserAsWatcherCommandConsumer;
import org.moshe.arad.kafka.consumers.commands.LeaveGameRoomCommandConsumer;
import org.moshe.arad.kafka.consumers.commands.OpenNewGameRoomCommandConsumer;
import org.moshe.arad.kafka.consumers.config.AddUserAsSecondPlayerCommandConfig;
import org.moshe.arad.kafka.consumers.config.AddUserAsWatcherCommandConfig;
import org.moshe.arad.kafka.consumers.config.FromMongoWithoutSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.InitDiceCompletedEventConfig;
import org.moshe.arad.kafka.consumers.config.LeaveGameRoomCommandConfig;
import org.moshe.arad.kafka.consumers.config.LoggedInEventAckConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutOpenByLeftBeforeGameStartedEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutOpenByLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutSecondLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutWatcherLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.NewUserCreatedEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenByLeftBeforeGameStartedEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenByLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenNewGameRoomCommandConfig;
import org.moshe.arad.kafka.consumers.config.SecondLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.consumers.config.UserAddedAsSecondPlayerEventConfig;
import org.moshe.arad.kafka.consumers.config.WatcherLeftLastEventConfig;
import org.moshe.arad.kafka.consumers.events.FromMongoWithSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.FromMongoWithoutSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.InitDiceCompletedEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedInEventAckConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutOpenByLeftBeforeGameStartedEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutOpenByLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutSecondLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutWatcherLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.NewUserCreatedEventAckConsumer;
import org.moshe.arad.kafka.consumers.events.OpenByLeftBeforeGameStartedEventConsumer;
import org.moshe.arad.kafka.consumers.events.OpenByLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.SecondLeftLastEventConsumer;
import org.moshe.arad.kafka.consumers.events.UserAddedAsSecondPlayerEventConsumer;
import org.moshe.arad.kafka.consumers.events.WatcherLeftLastEventConsumer;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.InitGameRoomCompletedEvent;
import org.moshe.arad.kafka.events.LoggedOutEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutOpenByLeftLastEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftFirstEvent;
import org.moshe.arad.kafka.events.LoggedOutSecondLeftLastEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEventAck;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.OpenByLeftBeforeGameStartedEvent;
import org.moshe.arad.kafka.events.OpenByLeftEvent;
import org.moshe.arad.kafka.events.OpenByLeftFirstEvent;
import org.moshe.arad.kafka.events.OpenByLeftLastEvent;
import org.moshe.arad.kafka.events.RollDiceGameRoomFoundEvent;
import org.moshe.arad.kafka.events.SecondLeftEvent;
import org.moshe.arad.kafka.events.SecondLeftFirstEvent;
import org.moshe.arad.kafka.events.SecondLeftLastEvent;
import org.moshe.arad.kafka.events.UserAddedAsSecondPlayerEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
import org.moshe.arad.kafka.events.WatcherLeftEvent;
import org.moshe.arad.kafka.events.WatcherLeftLastEvent;
import org.moshe.arad.kafka.events.LoggedOutUserLeftLobbyEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftEvent;
import org.moshe.arad.kafka.events.LoggedOutWatcherLeftLastEvent;
import org.moshe.arad.kafka.producers.ISimpleProducer;
import org.moshe.arad.kafka.producers.commands.ISimpleCommandProducer;
import org.moshe.arad.kafka.producers.commands.SimpleCommandsProducer;
import org.moshe.arad.kafka.producers.events.SimpleEventsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class AppInit implements ApplicationContextAware, IAppInitializer {	
	
	private NewUserCreatedEventAckConsumer newUserCreatedEventAckConsumer;
	
	@Autowired
	private NewUserCreatedEventConfig newUserCreatedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<NewUserJoinedLobbyEvent> newUserJoinedLobbyEventsProducer;
	
	private LoggedInEventAckConsumer loggedInEventAckConsumer;
	
	@Autowired
	private LoggedInEventAckConfig loggedInEventAckConfig;
	
	@Autowired
	private SimpleEventsProducer<ExistingUserJoinedLobbyEvent> existingUserJoinedLobbyEventsProducer;
	
	@Autowired
	private SimpleCommandsProducer<PullEventsWithSavingCommand> pullEventsWithSavingCommandsProducer;
	
	private FromMongoWithSavingEventsConsumer fromMongoWithSavingEventsConsumer;
	
	private FromMongoWithoutSavingEventsConsumer fromMongoWithoutSavingEventsConsumer;
	
	@Autowired
	private FromMongoWithoutSavingEventsConfig fromMongoWithoutSavingEventsConfig;
	
	private OpenNewGameRoomCommandConsumer openNewGameRoomCommandConsumer;
	
	@Autowired
	private OpenNewGameRoomCommandConfig openNewGameRoomCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<NewGameRoomOpenedEvent> newGameRoomOpenedEventProducer;
	
	@Autowired
	private SimpleEventsProducer<NewGameRoomOpenedEventAck> newGameRoomOpenedEventAckProducer;
	
	private AddUserAsWatcherCommandConsumer addUserAsWatcherCommandConsumer;
	
	@Autowired
	private AddUserAsWatcherCommandConfig addUserAsWatcherCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<UserAddedAsWatcherEvent> userAddedAsWatcherEventProducer;
	
	private AddUserAsSecondPlayerCommandConsumer addUserAsSecondPlayerCommandConsumer;
	
	@Autowired
	private AddUserAsSecondPlayerCommandConfig addUserAsSecondPlayerCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<UserAddedAsSecondPlayerEvent> userAddedAsSecondPlayerEventProducer;
	
	private LoggedOutEventConsumer loggedOutEventConsumer;
	
	@Autowired
	private LoggedOutEventConfig loggedOutEventConfig;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutUserLeftLobbyEvent> loggedOutUserLeftLobbyEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutOpenByLeftBeforeGameStartedEvent> loggedOutOpenByLeftBeforeGameStartedEventProducer;
	
	private LoggedOutOpenByLeftBeforeGameStartedEventConsumer loggedOutOpenByLeftBeforeGameStartedEventConsumer;
	
	@Autowired
	private LoggedOutOpenByLeftBeforeGameStartedEventConfig loggedOutOpenByLeftBeforeGameStartedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutOpenByLeftEvent> loggedOutOpenByLeftEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutWatcherLeftLastEvent> loggedOutWatcherLeftLastEventProducer;

	private LoggedOutWatcherLeftLastEventConsumer loggedOutWatcherLeftLastEventConsumer;
	
	@Autowired
	private LoggedOutWatcherLeftLastEventConfig loggedOutWatcherLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedLoggedOutWatcherLeftLastEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutWatcherLeftEvent> loggedOutWatcherLeftEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutOpenByLeftFirstEvent> loggedOutOpenByLeftFirstEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutSecondLeftFirstEvent> loggedOutSecondLeftFirstEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutSecondLeftEvent> loggedOutSecondLeftEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutOpenByLeftLastEvent> loggedOutOpenByLeftLastEventProducer;
	
	private LoggedOutOpenByLeftLastEventConsumer loggedOutOpenByLeftLastEventConsumer;
	
	@Autowired
	private LoggedOutOpenByLeftLastEventConfig loggedOutOpenByLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedLoggedOutOpenByLeftLastEventProducer;
	
	@Autowired
	private SimpleEventsProducer<LoggedOutSecondLeftLastEvent> loggedOutSecondLeftLastEventProducer;
	
	private LoggedOutSecondLeftLastEventConsumer loggedOutSecondLeftLastEventConsumer;
	
	@Autowired
	private LoggedOutSecondLeftLastEventConfig loggedOutSecondLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedLoggedOutSecondLeftLastEventProducer;
	
	private LeaveGameRoomCommandConsumer leaveGameRoomCommandConsumer;
	
	@Autowired
	private LeaveGameRoomCommandConfig leaveGameRoomCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<OpenByLeftBeforeGameStartedEvent> openByLeftBeforeGameStartedEventProducer;
	
	private OpenByLeftBeforeGameStartedEventConsumer openByLeftBeforeGameStartedEventConsumer;
	
	@Autowired
	private OpenByLeftBeforeGameStartedEventConfig openByLeftBeforeGameStartedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedOpenByLeftBeforeGameStartedEventProducer;
	
	@Autowired
	private SimpleEventsProducer<OpenByLeftEvent> openByLeftEventProducer;
	
	@Autowired
	private SimpleEventsProducer<WatcherLeftLastEvent> watcherLeftLastEventProducer;
	
	private WatcherLeftLastEventConsumer watcherLeftLastEventConsumer;
	
	@Autowired
	private WatcherLeftLastEventConfig watcherLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedWatcherLeftLastEventProducer;
	
	@Autowired
	private SimpleEventsProducer<WatcherLeftEvent> watcherLeftEventProducer;
	
	@Autowired
	private SimpleEventsProducer<OpenByLeftFirstEvent> openByLeftFirstEventProducer;
	
	@Autowired
	private SimpleEventsProducer<SecondLeftFirstEvent> secondLeftFirstEventProducer;
	
	@Autowired
	private SimpleEventsProducer<SecondLeftEvent> secondLeftEventProducer;
	
	@Autowired
	private SimpleEventsProducer<OpenByLeftLastEvent> openByLeftLastEventProducer;
	
	private OpenByLeftLastEventConsumer openByLeftLastEventConsumer;
	
	@Autowired
	private OpenByLeftLastEventConfig openByLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedOpenByLeftLastEventProducer;
	
	@Autowired
	private SimpleEventsProducer<SecondLeftLastEvent> secondLeftLastEventProducer;
	
	private SecondLeftLastEventConsumer secondLeftLastEventConsumer;
	
	@Autowired
	private SecondLeftLastEventConfig secondLeftLastEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedSecondLeftLastEventProducer;
	
	private UserAddedAsSecondPlayerEventConsumer userAddedAsSecondPlayerEventConsumer;
	
	@Autowired
	private UserAddedAsSecondPlayerEventConfig userAddedAsSecondPlayerEventConfig;
	
	@Autowired
	private SimpleEventsProducer<InitGameRoomCompletedEvent> initGameRoomCompletedEventProducer;
	
	private InitDiceCompletedEventConsumer initDiceCompletedEventConsumer;
	
	@Autowired
	private InitDiceCompletedEventConfig initDiceCompletedEventConfig;
	
	@Autowired
	private SimpleEventsProducer<RollDiceGameRoomFoundEvent> rollDiceGameRoomFoundEventProducer;
	
	private ExecutorService executor = Executors.newFixedThreadPool(6);
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private ConsumerToProducerQueue loggedInEventAckQueue;
	
	private ConsumerToProducerQueue newGameRoomOpenQueue;
	
	private ConsumerToProducerQueue newGameRoomOpenAckQueue;
	
	private ConsumerToProducerQueue addWatcherQueue;
	
	private ConsumerToProducerQueue addSecondPlayerQueue;
	
	private ConsumerToProducerQueue userLeftLobbyQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftBeforeGameStartedQueue;
	
	private ConsumerToProducerQueue gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftEventQueue;
	
	private ConsumerToProducerQueue loggedOutWatcherLeftLastQueue;
	
	private ConsumerToProducerQueue gameRoomClosedLoggedOutWatcherLeftLastQueue;
	
	private ConsumerToProducerQueue loggedOutWatcherLeftQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftFirstQueue;
	
	private ConsumerToProducerQueue loggedOutSecondLeftFirstQueue;
	
	private ConsumerToProducerQueue loggedOutSecondLeftQueue;
	
	private ConsumerToProducerQueue loggedOutOpenByLeftLastQueue;
	
	private ConsumerToProducerQueue gameRoomClosedLoggedOutOpenByLeftLastQueue;
	
	private ConsumerToProducerQueue loggedOutSecondLeftLastQueue;
	
	private ConsumerToProducerQueue gameRoomClosedLoggedOutSecondLeftLastQueue;
	
	private ConsumerToProducerQueue openByLeftBeforeGameStartedQueue;
	
	private ConsumerToProducerQueue gameRoomClosedOpenByLeftBeforeGameStartedQueue;
	
	private ConsumerToProducerQueue openByLeftQueue;
	
	private ConsumerToProducerQueue watcherLeftLastQueue;
	
	private ConsumerToProducerQueue gameRoomClosedWatcherLeftLastQueue;
	
	private ConsumerToProducerQueue watcherLeftQueue;
	
	private ConsumerToProducerQueue openByLeftFirstQueue;
	
	private ConsumerToProducerQueue secondLeftFirstQueue;
	
	private ConsumerToProducerQueue secondLeftQueue;
	
	private ConsumerToProducerQueue openByLeftLastQueue;
	
	private ConsumerToProducerQueue gameRoomClosedOpenByLeftLastQueue;
	
	private ConsumerToProducerQueue secondLeftLastQueue;
	
	private ConsumerToProducerQueue gameRoomClosedSecondLeftLastQueue;
	
	private ConsumerToProducerQueue initGameRoomCompletedEventQueue;
	
	private ConsumerToProducerQueue initDiceCompletedEventQueue;
	
	public static final int NUM_CONSUMERS = 3;
	
	@Override
	public void initKafkaCommandsConsumers() {
		newGameRoomOpenQueue = context.getBean(ConsumerToProducerQueue.class);
		newGameRoomOpenAckQueue = context.getBean(ConsumerToProducerQueue.class); 
		addWatcherQueue = context.getBean(ConsumerToProducerQueue.class);
		addSecondPlayerQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftBeforeGameStartedQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		watcherLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		watcherLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		secondLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		secondLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		openByLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		secondLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			openNewGameRoomCommandConsumer = context.getBean(OpenNewGameRoomCommandConsumer.class);
			initSingleConsumer(openNewGameRoomCommandConsumer, KafkaUtils.OPEN_NEW_GAME_ROOM_COMMAND_TOPIC, openNewGameRoomCommandConfig, newGameRoomOpenQueue);
			
			addUserAsWatcherCommandConsumer = context.getBean(AddUserAsWatcherCommandConsumer.class);		
			initSingleConsumer(addUserAsWatcherCommandConsumer, KafkaUtils.ADD_USER_AS_WATCHER_COMMAND_TOPIC, addUserAsWatcherCommandConfig, addWatcherQueue);
			
			addUserAsSecondPlayerCommandConsumer = context.getBean(AddUserAsSecondPlayerCommandConsumer.class);
			initSingleConsumer(addUserAsSecondPlayerCommandConsumer, KafkaUtils.ADD_USER_AS_SECOND_PLAYER_COMMAND_TOPIC, addUserAsSecondPlayerCommandConfig, addSecondPlayerQueue);
			
			leaveGameRoomCommandConsumer = context.getBean(LeaveGameRoomCommandConsumer.class);
			HashMap<Class<? extends BackgammonEvent>, ConsumerToProducerQueue> queueMap = new HashMap<>(10000);
			queueMap.put(LoggedOutOpenByLeftBeforeGameStartedEvent.class, openByLeftBeforeGameStartedQueue);
			queueMap.put(OpenByLeftEvent.class, openByLeftQueue);
			queueMap.put(WatcherLeftLastEvent.class, watcherLeftLastQueue);
			queueMap.put(WatcherLeftEvent.class, watcherLeftQueue);
			queueMap.put(OpenByLeftFirstEvent.class, openByLeftFirstQueue);
			queueMap.put(SecondLeftFirstEvent.class, secondLeftFirstQueue);
			queueMap.put(SecondLeftEvent.class, secondLeftQueue);
			queueMap.put(OpenByLeftLastEvent.class, openByLeftLastQueue);
			queueMap.put(SecondLeftLastEvent.class, secondLeftLastQueue);			
			leaveGameRoomCommandConsumer.setConsumerToProducer(queueMap);
			
			initSingleConsumer(leaveGameRoomCommandConsumer, KafkaUtils.LEAVE_GAME_ROOM_COMMAND_TOPIC, leaveGameRoomCommandConfig);
			
			executeProducersAndConsumers(Arrays.asList(openNewGameRoomCommandConsumer, 
					addUserAsWatcherCommandConsumer,
					addUserAsSecondPlayerCommandConsumer,
					leaveGameRoomCommandConsumer));
		}
	}

	@Override
	public void initKafkaEventsConsumers() {	
		consumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedInEventAckQueue = context.getBean(ConsumerToProducerQueue.class); 
		userLeftLobbyQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftBeforeGameStartedQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftEventQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutWatcherLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedLoggedOutWatcherLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutWatcherLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutSecondLeftFirstQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutSecondLeftQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutOpenByLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedLoggedOutOpenByLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedOutSecondLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedLoggedOutSecondLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedOpenByLeftBeforeGameStartedQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedWatcherLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedOpenByLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		gameRoomClosedSecondLeftLastQueue = context.getBean(ConsumerToProducerQueue.class);
		initGameRoomCompletedEventQueue = context.getBean(ConsumerToProducerQueue.class);
		initDiceCompletedEventQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			newUserCreatedEventAckConsumer = context.getBean(NewUserCreatedEventAckConsumer.class);
			
			logger.info("Initializing new user created event consumer...");
			initSingleConsumer(newUserCreatedEventAckConsumer, KafkaUtils.NEW_USER_CREATED_EVENT_ACK_TOPIC, newUserCreatedEventConfig, consumerToProducerQueue);
			logger.info("Initialize new user created event, completed...");
			
			loggedInEventAckConsumer = context.getBean(LoggedInEventAckConsumer.class);
			initSingleConsumer(loggedInEventAckConsumer, KafkaUtils.LOGGED_IN_EVENT_ACK_TOPIC, loggedInEventAckConfig, loggedInEventAckQueue);
			
			fromMongoWithoutSavingEventsConsumer = context.getBean(FromMongoWithoutSavingEventsConsumer.class);
			initSingleConsumer(fromMongoWithoutSavingEventsConsumer, KafkaUtils.TO_LOBBY_FROM_MONGO_EVENTS_WITHOUT_SAVING_TOPIC, fromMongoWithoutSavingEventsConfig);
			
			loggedOutEventConsumer = context.getBean(LoggedOutEventConsumer.class);
			
			HashMap<Class<? extends BackgammonEvent>, ConsumerToProducerQueue> queueMap = new HashMap<>(10000);
			queueMap.put(LoggedOutUserLeftLobbyEvent.class, userLeftLobbyQueue); 
			queueMap.put(LoggedOutOpenByLeftBeforeGameStartedEvent.class, loggedOutOpenByLeftBeforeGameStartedQueue);
			queueMap.put(LoggedOutOpenByLeftEvent.class, loggedOutOpenByLeftEventQueue);
			queueMap.put(LoggedOutWatcherLeftLastEvent.class, loggedOutWatcherLeftLastQueue);
			queueMap.put(LoggedOutWatcherLeftEvent.class, loggedOutWatcherLeftQueue);
			queueMap.put(LoggedOutOpenByLeftFirstEvent.class, loggedOutOpenByLeftFirstQueue);
			queueMap.put(LoggedOutSecondLeftFirstEvent.class, loggedOutSecondLeftFirstQueue);
			queueMap.put(LoggedOutSecondLeftEvent.class, loggedOutSecondLeftQueue);
			queueMap.put(LoggedOutOpenByLeftLastEvent.class, loggedOutOpenByLeftLastQueue);
			queueMap.put(LoggedOutSecondLeftLastEvent.class, loggedOutSecondLeftLastQueue);
			loggedOutEventConsumer.setConsumerToProducer(queueMap);
			
			initSingleConsumer(loggedOutEventConsumer, KafkaUtils.LOGGED_OUT_EVENT_TOPIC, loggedOutEventConfig);
			
			loggedOutOpenByLeftBeforeGameStartedEventConsumer = context.getBean(LoggedOutOpenByLeftBeforeGameStartedEventConsumer.class);
			initSingleConsumer(loggedOutOpenByLeftBeforeGameStartedEventConsumer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, loggedOutOpenByLeftBeforeGameStartedEventConfig, gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedQueue);
			
			loggedOutWatcherLeftLastEventConsumer = context.getBean(LoggedOutWatcherLeftLastEventConsumer.class);
			initSingleConsumer(loggedOutWatcherLeftLastEventConsumer, KafkaUtils.LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC, loggedOutWatcherLeftLastEventConfig, gameRoomClosedLoggedOutWatcherLeftLastQueue);
			
			loggedOutOpenByLeftLastEventConsumer = context.getBean(LoggedOutOpenByLeftLastEventConsumer.class);
			initSingleConsumer(loggedOutOpenByLeftLastEventConsumer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC, loggedOutOpenByLeftLastEventConfig, gameRoomClosedLoggedOutOpenByLeftLastQueue);
			
			loggedOutSecondLeftLastEventConsumer = context.getBean(LoggedOutSecondLeftLastEventConsumer.class);
			initSingleConsumer(loggedOutSecondLeftLastEventConsumer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC, loggedOutSecondLeftLastEventConfig, gameRoomClosedLoggedOutSecondLeftLastQueue);
			
			openByLeftBeforeGameStartedEventConsumer = context.getBean(OpenByLeftBeforeGameStartedEventConsumer.class);
			initSingleConsumer(openByLeftBeforeGameStartedEventConsumer, KafkaUtils.OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, openByLeftBeforeGameStartedEventConfig, gameRoomClosedOpenByLeftBeforeGameStartedQueue);
			
			watcherLeftLastEventConsumer = context.getBean(WatcherLeftLastEventConsumer.class);
			initSingleConsumer(watcherLeftLastEventConsumer, KafkaUtils.WATCHER_LEFT_LAST_EVENT_TOPIC, watcherLeftLastEventConfig, gameRoomClosedWatcherLeftLastQueue);
			
			openByLeftLastEventConsumer = context.getBean(OpenByLeftLastEventConsumer.class);
			initSingleConsumer(openByLeftLastEventConsumer, KafkaUtils.OPENBY_LEFT_LAST_EVENT_TOPIC, openByLeftLastEventConfig, gameRoomClosedOpenByLeftLastQueue);
			
			secondLeftLastEventConsumer = context.getBean(SecondLeftLastEventConsumer.class);
			initSingleConsumer(secondLeftLastEventConsumer, KafkaUtils.SECOND_LEFT_LAST_EVENT_TOPIC, secondLeftLastEventConfig, gameRoomClosedSecondLeftLastQueue);
			
			userAddedAsSecondPlayerEventConsumer =context.getBean(UserAddedAsSecondPlayerEventConsumer.class);
			initSingleConsumer(userAddedAsSecondPlayerEventConsumer, KafkaUtils.USER_ADDED_AS_SECOND_PLAYER_EVENT_TOPIC, userAddedAsSecondPlayerEventConfig, initGameRoomCompletedEventQueue);
			
			initDiceCompletedEventConsumer = context.getBean(InitDiceCompletedEventConsumer.class);
			initSingleConsumer(initDiceCompletedEventConsumer, KafkaUtils.INIT_DICE_COMPLETED_EVENT_TOPIC, initDiceCompletedEventConfig, initDiceCompletedEventQueue);
			
			executeProducersAndConsumers(Arrays.asList(newUserCreatedEventAckConsumer, 
					loggedInEventAckConsumer,
					fromMongoWithoutSavingEventsConsumer,
					loggedOutEventConsumer,
					loggedOutOpenByLeftBeforeGameStartedEventConsumer,
					loggedOutWatcherLeftLastEventConsumer,
					loggedOutOpenByLeftLastEventConsumer,
					loggedOutSecondLeftLastEventConsumer,
					openByLeftBeforeGameStartedEventConsumer,
					watcherLeftLastEventConsumer,
					openByLeftLastEventConsumer,
					secondLeftLastEventConsumer,
					userAddedAsSecondPlayerEventConsumer,
					initDiceCompletedEventConsumer));
		}
	}

	@Override
	public void initKafkaCommandsProducers() {
		initSingleProducer(pullEventsWithSavingCommandsProducer, 20, 20, TimeUnit.MINUTES, KafkaUtils.LOBBY_SERVICE_PULL_EVENTS_WITH_SAVING_COMMAND_TOPIC, null);
		
		executeProducersAndConsumers(Arrays.asList(pullEventsWithSavingCommandsProducer));
	}

	@Override
	public void initKafkaEventsProducers() {
		logger.info("Initializing new user created event consumer...");
		initSingleProducer(newUserJoinedLobbyEventsProducer, KafkaUtils.NEW_USER_JOINED_LOBBY_EVENT_TOPIC, consumerToProducerQueue);
		logger.info("Initialize new user created event, completed...");
		
		initSingleProducer(existingUserJoinedLobbyEventsProducer, KafkaUtils.EXISTING_USER_JOINED_LOBBY_EVENT_TOPIC, loggedInEventAckQueue);
				
		initSingleProducer(newGameRoomOpenedEventProducer, KafkaUtils.NEW_GAME_ROOM_OPENED_EVENT_TOPIC, newGameRoomOpenQueue);
		
		initSingleProducer(newGameRoomOpenedEventAckProducer, KafkaUtils.NEW_GAME_ROOM_OPENED_EVENT_ACK_TOPIC, newGameRoomOpenAckQueue);
		
		initSingleProducer(userAddedAsWatcherEventProducer, KafkaUtils.USER_ADDED_AS_WATCHER_EVENT_TOPIC, addWatcherQueue);
		
		initSingleProducer(userAddedAsSecondPlayerEventProducer, KafkaUtils.USER_ADDED_AS_SECOND_PLAYER_EVENT_TOPIC, addSecondPlayerQueue);
		
		initSingleProducer(loggedOutUserLeftLobbyEventProducer, KafkaUtils.LOGGED_OUT_USER_LEFT_LOBBY_EVENT_TOPIC, userLeftLobbyQueue);

		initSingleProducer(loggedOutOpenByLeftBeforeGameStartedEventProducer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, loggedOutOpenByLeftBeforeGameStartedQueue);
		
		initSingleProducer(gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedEventProducer, KafkaUtils.GAME_ROOM_CLOSED_LOGGED_OUT_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedQueue);
		
		initSingleProducer(loggedOutOpenByLeftEventProducer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_EVENT_TOPIC, loggedOutOpenByLeftEventQueue);
		
		initSingleProducer(loggedOutWatcherLeftLastEventProducer, KafkaUtils.LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC, loggedOutWatcherLeftLastQueue);
		
		initSingleProducer(gameRoomClosedLoggedOutWatcherLeftLastEventProducer, KafkaUtils.GAME_ROOM_CLOSED_LOGGED_OUT_WATCHER_LEFT_LAST_EVENT_TOPIC, gameRoomClosedLoggedOutWatcherLeftLastQueue);
		
		initSingleProducer(loggedOutWatcherLeftEventProducer, KafkaUtils.LOGGED_OUT_WATCHER_LEFT_EVENT_TOPIC, loggedOutWatcherLeftQueue);
		
		initSingleProducer(loggedOutOpenByLeftFirstEventProducer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_FIRST_EVENT_TOPIC, loggedOutOpenByLeftFirstQueue);
		
		initSingleProducer(loggedOutSecondLeftFirstEventProducer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_FIRST_EVENT_TOPIC, loggedOutSecondLeftFirstQueue);
		
		initSingleProducer(loggedOutSecondLeftEventProducer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_EVENT_TOPIC, loggedOutSecondLeftQueue);
		
		initSingleProducer(loggedOutOpenByLeftLastEventProducer, KafkaUtils.LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC, loggedOutOpenByLeftLastQueue);
		
		initSingleProducer(gameRoomClosedLoggedOutOpenByLeftLastEventProducer, KafkaUtils.GAME_ROOM_CLOSED_LOGGED_OUT_OPENBY_LEFT_LAST_EVENT_TOPIC, gameRoomClosedLoggedOutOpenByLeftLastQueue);
		
		initSingleProducer(loggedOutSecondLeftLastEventProducer, KafkaUtils.LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC, loggedOutSecondLeftLastQueue);
		
		initSingleProducer(gameRoomClosedLoggedOutSecondLeftLastEventProducer, KafkaUtils.GAME_ROOM_CLOSED_LOGGED_OUT_SECOND_LEFT_LAST_EVENT_TOPIC, gameRoomClosedLoggedOutSecondLeftLastQueue);
		
		initSingleProducer(openByLeftBeforeGameStartedEventProducer, KafkaUtils.OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, openByLeftBeforeGameStartedQueue);
		
		initSingleProducer(gameRoomClosedOpenByLeftBeforeGameStartedEventProducer, KafkaUtils.GAME_ROOM_CLOSED_OPENBY_LEFT_BEFORE_GAME_STARTED_EVENT_TOPIC, gameRoomClosedOpenByLeftBeforeGameStartedQueue);
		
		initSingleProducer(openByLeftEventProducer, KafkaUtils.OPENBY_LEFT_EVENT_TOPIC, openByLeftQueue);
		
		initSingleProducer(watcherLeftLastEventProducer, KafkaUtils.WATCHER_LEFT_LAST_EVENT_TOPIC, watcherLeftLastQueue);
		
		initSingleProducer(gameRoomClosedWatcherLeftLastEventProducer, KafkaUtils.GAME_ROOM_CLOSED_WATCHER_LEFT_LAST_EVENT_TOPIC, gameRoomClosedWatcherLeftLastQueue);
		
		initSingleProducer(watcherLeftEventProducer, KafkaUtils.WATCHER_LEFT_EVENT_TOPIC, watcherLeftQueue);
		
		initSingleProducer(openByLeftFirstEventProducer, KafkaUtils.OPENBY_LEFT_FIRST_EVENT_TOPIC, openByLeftFirstQueue);
		
		initSingleProducer(secondLeftFirstEventProducer, KafkaUtils.SECOND_LEFT_FIRST_EVENT_TOPIC, secondLeftFirstQueue);
		
		initSingleProducer(secondLeftEventProducer, KafkaUtils.SECOND_LEFT_EVENT_TOPIC, secondLeftQueue);
		
		initSingleProducer(openByLeftLastEventProducer, KafkaUtils.OPENBY_LEFT_LAST_EVENT_TOPIC, openByLeftLastQueue);
		
		initSingleProducer(gameRoomClosedOpenByLeftLastEventProducer, KafkaUtils.GAME_ROOM_CLOSED_OPENBY_LEFT_LAST_EVENT_TOPIC, gameRoomClosedOpenByLeftLastQueue);
		
		initSingleProducer(secondLeftLastEventProducer, KafkaUtils.SECOND_LEFT_LAST_EVENT_TOPIC, secondLeftLastQueue);
		
		initSingleProducer(gameRoomClosedSecondLeftLastEventProducer, KafkaUtils.GAME_ROOM_CLOSED_SECOND_LEFT_LAST_EVENT_TOPIC, gameRoomClosedSecondLeftLastQueue);
		
		initSingleProducer(initGameRoomCompletedEventProducer, KafkaUtils.INIT_GAME_ROOM_COMPLETED_EVENT_TOPIC, initGameRoomCompletedEventQueue);
		
		initSingleProducer(rollDiceGameRoomFoundEventProducer, KafkaUtils.ROLL_DICE_GAME_ROOM_FOUND_EVENT_TOPIC, initDiceCompletedEventQueue);
		
		executeProducersAndConsumers(Arrays.asList(newUserJoinedLobbyEventsProducer, 
				existingUserJoinedLobbyEventsProducer,
				newGameRoomOpenedEventProducer,
				newGameRoomOpenedEventAckProducer,
				userAddedAsWatcherEventProducer,
				userAddedAsSecondPlayerEventProducer,
				loggedOutUserLeftLobbyEventProducer,
				loggedOutOpenByLeftBeforeGameStartedEventProducer,
				gameRoomClosedLoggedOutOpenByLeftBeforeGameStartedEventProducer,
				loggedOutOpenByLeftEventProducer,
				loggedOutWatcherLeftLastEventProducer,
				gameRoomClosedLoggedOutWatcherLeftLastEventProducer,
				loggedOutWatcherLeftEventProducer,
				loggedOutOpenByLeftFirstEventProducer,
				loggedOutSecondLeftFirstEventProducer,
				loggedOutSecondLeftEventProducer,
				loggedOutOpenByLeftLastEventProducer,
				gameRoomClosedLoggedOutOpenByLeftLastEventProducer,
				loggedOutSecondLeftLastEventProducer,
				gameRoomClosedLoggedOutSecondLeftLastEventProducer,
				openByLeftBeforeGameStartedEventProducer,
				gameRoomClosedOpenByLeftBeforeGameStartedEventProducer,
				openByLeftEventProducer,
				watcherLeftLastEventProducer,
				gameRoomClosedWatcherLeftLastEventProducer,
				watcherLeftEventProducer,
				openByLeftFirstEventProducer,
				secondLeftFirstEventProducer,
				secondLeftEventProducer,
				openByLeftLastEventProducer,
				gameRoomClosedOpenByLeftLastEventProducer,
				secondLeftLastEventProducer,
				gameRoomClosedSecondLeftLastEventProducer,
				initGameRoomCompletedEventProducer,
				rollDiceGameRoomFoundEventProducer));		
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}

	@Override
	public void engineShutdown() {
		logger.info("about to do shutdown.");	
		shutdownSingleConsumer(newUserCreatedEventAckConsumer);
		shutdownSingleProducer(newUserJoinedLobbyEventsProducer);
		selfShutdown();
		logger.info("shutdown compeleted.");
	}	
	
	private void initSingleConsumer(ISimpleConsumer consumer, String topic, SimpleConsumerConfig consumerConfig, ConsumerToProducerQueue queue) {
		consumer.setTopic(topic);
		consumer.setSimpleConsumerConfig(consumerConfig);
		consumer.initConsumer();	
		consumer.setConsumerToProducerQueue(queue);
	}
	
	private void initSingleConsumer(ISimpleConsumer consumer, String topic, SimpleConsumerConfig consumerConfig) {
		consumer.setTopic(topic);
		consumer.setSimpleConsumerConfig(consumerConfig);
		consumer.initConsumer();	
	}
	
	private void initSingleProducer(ISimpleProducer producer, String topic, ConsumerToProducerQueue queue) {
		producer.setTopic(topic);	
		producer.setConsumerToProducerQueue(queue);
	}
	
	private void initSingleProducer(ISimpleCommandProducer producer, int period, int initialDelay, TimeUnit timeUnit, String topic, ConsumerToProducerQueue queue) {
		producer.setPeriodic(true);
		producer.setPeriod(period);
		producer.setInitialDelay(initialDelay);
		producer.setTimeUnit(timeUnit);
		producer.setTopic(topic);	
		producer.setConsumerToProducerQueue(queue);
	}
	
	private void shutdownSingleConsumer(ISimpleConsumer consumer) {
		consumer.setRunning(false);
		consumer.getScheduledExecutor().shutdown();	
		consumer.closeConsumer();
	}
	
	private void shutdownSingleProducer(ISimpleProducer producer) {
		producer.setRunning(false);
		producer.getScheduledExecutor().shutdown();	
	}
	
	private void selfShutdown(){
		this.executor.shutdown();
	}
	
	private void executeProducersAndConsumers(List<Runnable> jobs){
		for(Runnable job:jobs)
			executor.execute(job);
	}
}
