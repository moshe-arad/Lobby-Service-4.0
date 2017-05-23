package org.moshe.arad.initializer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.commands.PullEventsWithSavingCommand;
import org.moshe.arad.kafka.consumers.ISimpleConsumer;
import org.moshe.arad.kafka.consumers.commands.AddUserAsWatcherCommandConsumer;
import org.moshe.arad.kafka.consumers.commands.CloseGameRoomCommandConsumer;
import org.moshe.arad.kafka.consumers.commands.OpenNewGameRoomCommandConsumer;
import org.moshe.arad.kafka.consumers.config.AddUserAsWatcherCommandConfig;
import org.moshe.arad.kafka.consumers.config.CloseGameRoomCommandConfig;
import org.moshe.arad.kafka.consumers.config.FromMongoWithoutSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.LoggedInEventAckConfig;
import org.moshe.arad.kafka.consumers.config.LoggedOutEventConfig;
import org.moshe.arad.kafka.consumers.config.NewUserCreatedEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenNewGameRoomCommandConfig;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.consumers.events.FromMongoWithSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.FromMongoWithoutSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedInEventAckConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedOutEventConsumer;
import org.moshe.arad.kafka.consumers.events.NewUserCreatedEventAckConsumer;
import org.moshe.arad.kafka.events.CloseGameRoomEventAck;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.GameRoomClosedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEventAck;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEvent;
import org.moshe.arad.kafka.events.UserAddedAsWatcherEventAck;
import org.moshe.arad.kafka.events.WatcherRemovedEvent;
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
	
	private CloseGameRoomCommandConsumer closeGameRoomCommandConsumer;
	
	@Autowired
	private CloseGameRoomCommandConfig closeGameRoomCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedEventProducer;
	
	@Autowired
	private SimpleEventsProducer<CloseGameRoomEventAck> closeGameRoomEventAckProducer;
	
	private AddUserAsWatcherCommandConsumer addUserAsWatcherCommandConsumer;
	
	@Autowired
	private AddUserAsWatcherCommandConfig addUserAsWatcherCommandConfig;
	
	@Autowired
	private SimpleEventsProducer<UserAddedAsWatcherEvent> userAddedAsWatcherEventProducer;
	
	@Autowired
	private SimpleEventsProducer<UserAddedAsWatcherEventAck> userAddedAsWatcherEventAckProducer;
	
	private LoggedOutEventConsumer loggedOutEventConsumer;
	
	@Autowired
	private LoggedOutEventConfig loggedOutEventConfig;
	
	@Autowired
	private SimpleEventsProducer<GameRoomClosedEvent> gameRoomClosedEventProducerLogout;
	
	@Autowired
	private SimpleEventsProducer<WatcherRemovedEvent> watcherRemovedEventProducer;
	
	private ExecutorService executor = Executors.newFixedThreadPool(6);
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private ConsumerToProducerQueue loggedInEventAckQueue;
	
	private ConsumerToProducerQueue newGameRoomOpenQueue;
	
	private ConsumerToProducerQueue newGameRoomOpenAckQueue;
	
	private ConsumerToProducerQueue closeGameRoomQueue;
	
	private ConsumerToProducerQueue closeGameRoomAckQueue;
	
	private ConsumerToProducerQueue addWatcherQueue;
	
	private ConsumerToProducerQueue addWatcherAckQueue;
	
	private ConsumerToProducerQueue logoutWatcherQueue;
	
	private ConsumerToProducerQueue logoutOpenedByQueue;
	
	public static final int NUM_CONSUMERS = 3;
	
	@Override
	public void initKafkaCommandsConsumers() {
		newGameRoomOpenQueue = context.getBean(ConsumerToProducerQueue.class);
		newGameRoomOpenAckQueue = context.getBean(ConsumerToProducerQueue.class); 
		closeGameRoomQueue = context.getBean(ConsumerToProducerQueue.class);
		closeGameRoomAckQueue = context.getBean(ConsumerToProducerQueue.class);
		addWatcherQueue = context.getBean(ConsumerToProducerQueue.class);
		addWatcherAckQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			openNewGameRoomCommandConsumer = context.getBean(OpenNewGameRoomCommandConsumer.class);
			initSingleConsumer(openNewGameRoomCommandConsumer, KafkaUtils.OPEN_NEW_GAME_ROOM_COMMAND_TOPIC, openNewGameRoomCommandConfig, newGameRoomOpenQueue);
			
			closeGameRoomCommandConsumer = context.getBean(CloseGameRoomCommandConsumer.class);
			closeGameRoomCommandConsumer.setConsumerToProducerAckQueue(closeGameRoomAckQueue);
			initSingleConsumer(closeGameRoomCommandConsumer, KafkaUtils.CLOSE_GAME_ROOM_COMMAND_TOPIC, closeGameRoomCommandConfig, closeGameRoomQueue);
			
			addUserAsWatcherCommandConsumer = context.getBean(AddUserAsWatcherCommandConsumer.class);
			addUserAsWatcherCommandConsumer.setConsumerToProducerAckQueue(addWatcherAckQueue);			
			initSingleConsumer(addUserAsWatcherCommandConsumer, KafkaUtils.ADD_USER_AS_WATCHER_COMMAND_TOPIC, addUserAsWatcherCommandConfig, addWatcherQueue);
			
			executeProducersAndConsumers(Arrays.asList(openNewGameRoomCommandConsumer, 
					closeGameRoomCommandConsumer, addUserAsWatcherCommandConsumer));
		}
	}

	@Override
	public void initKafkaEventsConsumers() {	
		consumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedInEventAckQueue = context.getBean(ConsumerToProducerQueue.class);
		logoutWatcherQueue = context.getBean(ConsumerToProducerQueue.class);
		logoutOpenedByQueue = context.getBean(ConsumerToProducerQueue.class); 
		
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
			loggedOutEventConsumer.setConsumerToProducerOpenedByQueue(logoutOpenedByQueue);
			loggedOutEventConsumer.setConsumerToProducerWatcherQueue(logoutWatcherQueue);
			initSingleConsumer(loggedOutEventConsumer, KafkaUtils.LOGGED_OUT_EVENT_TOPIC, loggedOutEventConfig);
			
			executeProducersAndConsumers(Arrays.asList(newUserCreatedEventAckConsumer, 
					loggedInEventAckConsumer,
					fromMongoWithoutSavingEventsConsumer,
					loggedOutEventConsumer));
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
		
		initSingleProducer(gameRoomClosedEventProducer, KafkaUtils.GAME_ROOM_CLOSED_EVENT_TOPIC, closeGameRoomQueue);
		
		initSingleProducer(closeGameRoomEventAckProducer, KafkaUtils.CLOSE_GAME_ROOM_EVENT_ACK_TOPIC, closeGameRoomAckQueue);
		
		initSingleProducer(userAddedAsWatcherEventProducer, KafkaUtils.USER_ADDED_AS_WATCHER_EVENT_TOPIC, addWatcherQueue);
		
		initSingleProducer(userAddedAsWatcherEventAckProducer, KafkaUtils.USER_ADDED_AS_WATCHER_EVENT_ACK_TOPIC, addWatcherAckQueue);
		
		initSingleProducer(gameRoomClosedEventProducerLogout, KafkaUtils.GAME_ROOM_CLOSED_EVENT_LOGOUT_TOPIC, logoutOpenedByQueue);
		
		initSingleProducer(watcherRemovedEventProducer, KafkaUtils.WATCHER_REMOVED_EVENT_TOPIC, logoutWatcherQueue);
		
		executeProducersAndConsumers(Arrays.asList(newUserJoinedLobbyEventsProducer, 
				existingUserJoinedLobbyEventsProducer,
				newGameRoomOpenedEventProducer,
				newGameRoomOpenedEventAckProducer,
				gameRoomClosedEventProducer,
				closeGameRoomEventAckProducer,
				userAddedAsWatcherEventProducer,
				userAddedAsWatcherEventAckProducer,
				gameRoomClosedEventProducerLogout,
				watcherRemovedEventProducer));		
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
