package org.moshe.arad.kafka.producers;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.BackgammonEvent;

public interface ISimpleProducer <T extends BackgammonEvent>{

	public void sendKafkaMessage(T event);
	
	public void setTopic(String topic);
	public void setSimpleProducerConfig(SimpleProducerConfig simpleProducerConfig);
	public void setConsumerToProducerQueue(ConsumerToProducerQueue queue);
	public void setRunning(boolean isRunning);	
	public ScheduledThreadPoolExecutor getScheduledExecutor();
}
