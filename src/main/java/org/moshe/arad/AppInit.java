package org.moshe.arad;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.KafkaUtils;
import org.moshe.arad.kafka.consumers.config.NewUserCreatedEventConfig;
import org.moshe.arad.kafka.consumers.events.NewUserCreatedEventConsumer;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;
import org.moshe.arad.kafka.producers.SimpleBackgammonEventsProducer;
import org.moshe.arad.kafka.producers.config.NewUserJoinedLobbyEventConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class AppInit implements ApplicationContextAware {

	private ExecutorService executor = Executors.newFixedThreadPool(6);
	
	private Logger logger = LoggerFactory.getLogger(AppInit.class);
	
	private ApplicationContext context;
	
	@Autowired
	private NewUserCreatedEventConsumer newUserCreatedEventConsumer;
	
	@Autowired
	private NewUserCreatedEventConfig newUserCreatedEventConfig;
	
	@Autowired
	private SimpleBackgammonEventsProducer<NewUserJoinedLobbyEvent> newUserJoinedLobbyEventsProducer;
	
	@Autowired
	private NewUserJoinedLobbyEventConfig newUserJoinedLobbyEventConfig;
	
	public void acceptNewEvents(){
		ConsumerToProducerQueue consumerToProducerQueue = context.getBean(ConsumerToProducerQueue.class);
		
		newUserCreatedEventConsumer.setTopic(KafkaUtils.NEW_USER_CREATED_EVENT_TOPIC);
		newUserCreatedEventConsumer.setSimpleConsumerConfig(newUserCreatedEventConfig);
		newUserCreatedEventConsumer.initConsumer();
		newUserCreatedEventConsumer.setConsumerToProducerQueue(consumerToProducerQueue);
		executor.execute(newUserCreatedEventConsumer);
		
		newUserJoinedLobbyEventsProducer.setTopic(KafkaUtils.NEW_USER_JOINED_LOBBY_EVENT_TOPIC);
		newUserJoinedLobbyEventsProducer.setSimpleProducerConfig(newUserJoinedLobbyEventConfig);
		newUserJoinedLobbyEventsProducer.setConsumerToProducerQueue(consumerToProducerQueue);
		executor.execute(newUserJoinedLobbyEventsProducer);
	}
	
	public void shutdown(){
		newUserCreatedEventConsumer.setRunning(false);
		newUserCreatedEventConsumer.getScheduledExecutor().shutdown();
		
		newUserJoinedLobbyEventsProducer.setRunning(false);
		newUserJoinedLobbyEventsProducer.getScheduledExecutor().shutdown();
		
		this.executor.shutdown();
	}
	
	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
}
