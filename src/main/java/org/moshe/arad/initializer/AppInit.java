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
import org.moshe.arad.kafka.consumers.commands.OpenNewGameRoomCommandConsumer;
import org.moshe.arad.kafka.consumers.config.FromMongoWithoutSavingEventsConfig;
import org.moshe.arad.kafka.consumers.config.LoggedInEventAckConfig;
import org.moshe.arad.kafka.consumers.config.NewUserCreatedEventConfig;
import org.moshe.arad.kafka.consumers.config.OpenNewGameRoomCommandConfig;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;
import org.moshe.arad.kafka.consumers.events.FromMongoWithSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.FromMongoWithoutSavingEventsConsumer;
import org.moshe.arad.kafka.consumers.events.LoggedInEventAckConsumer;
import org.moshe.arad.kafka.consumers.events.NewUserCreatedEventAckConsumer;
import org.moshe.arad.kafka.events.ExistingUserJoinedLobbyEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEvent;
import org.moshe.arad.kafka.events.NewGameRoomOpenedEventAck;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
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
	
	private ExecutorService executor = Executors.newFixedThreadPool(6);
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	private ApplicationContext context;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	
	private ConsumerToProducerQueue loggedInEventAckQueue;
	
	private ConsumerToProducerQueue newGameRoomOpenQueue;
	
	private ConsumerToProducerQueue newGameRoomOpenAckQueue;
	
	public static final int NUM_CONSUMERS = 5;
	
	@Override
	public void initKafkaCommandsConsumers() {
		newGameRoomOpenQueue = context.getBean(ConsumerToProducerQueue.class);
		newGameRoomOpenAckQueue = context.getBean(ConsumerToProducerQueue.class); 
				
		for(int i=0; i<NUM_CONSUMERS; i++){
			openNewGameRoomCommandConsumer = context.getBean(OpenNewGameRoomCommandConsumer.class);
			openNewGameRoomCommandConsumer.setConsumerToProducerAckQueue(newGameRoomOpenAckQueue);
			
			initSingleConsumer(openNewGameRoomCommandConsumer, KafkaUtils.OPEN_NEW_GAME_ROOM_COMMAND_TOPIC, openNewGameRoomCommandConfig, newGameRoomOpenQueue);						
			executeProducersAndConsumers(Arrays.asList(openNewGameRoomCommandConsumer));
		}
	}

	@Override
	public void initKafkaEventsConsumers() {	
		consumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		loggedInEventAckQueue = context.getBean(ConsumerToProducerQueue.class);
		
		for(int i=0; i<NUM_CONSUMERS; i++){
			newUserCreatedEventAckConsumer = context.getBean(NewUserCreatedEventAckConsumer.class);
			logger.info("Initializing new user created event consumer...");
			initSingleConsumer(newUserCreatedEventAckConsumer, KafkaUtils.NEW_USER_CREATED_EVENT_ACK_TOPIC, newUserCreatedEventConfig, consumerToProducerQueue);
			logger.info("Initialize new user created event, completed...");
			
			loggedInEventAckConsumer = context.getBean(LoggedInEventAckConsumer.class);
			initSingleConsumer(loggedInEventAckConsumer, KafkaUtils.LOGGED_IN_EVENT_ACK_TOPIC, loggedInEventAckConfig, loggedInEventAckQueue);
			
			fromMongoWithoutSavingEventsConsumer = context.getBean(FromMongoWithoutSavingEventsConsumer.class);
			initSingleConsumer(fromMongoWithoutSavingEventsConsumer, KafkaUtils.TO_LOBBY_FROM_MONGO_EVENTS_WITHOUT_SAVING_TOPIC, fromMongoWithoutSavingEventsConfig);
			
			executeProducersAndConsumers(Arrays.asList(newUserCreatedEventAckConsumer, 
					loggedInEventAckConsumer,
					fromMongoWithoutSavingEventsConsumer));
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
		
		executeProducersAndConsumers(Arrays.asList(newUserJoinedLobbyEventsProducer, 
				existingUserJoinedLobbyEventsProducer,
				newGameRoomOpenedEventProducer,
				newGameRoomOpenedEventAckProducer));		
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
