package org.moshe.arad.kafka.producers.events;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.producers.ISimpleProducer;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;

public interface ISimpleEventProducer<T extends BackgammonEvent> extends ISimpleProducer<T>{

	public void sendKafkaMessage(T event);
	
	@Override
	public void setTopic(String topic);
	
	@Override
	public void setSimpleProducerConfig(SimpleProducerConfig simpleProducerConfig);
	
	@Override
	public void setConsumerToProducerQueue(ConsumerToProducerQueue queue);
	
	@Override
	public void setRunning(boolean isRunning);	
	
	@Override
	public ScheduledThreadPoolExecutor getScheduledExecutor();
}
