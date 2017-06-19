package org.moshe.arad.kafka.producers;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.events.BackgammonEvent;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;

public interface ISimpleProducer <T extends BackgammonEvent>{

	public void setPeriod(int num);
	public void setInitialDelay(int num);
	public void setTimeUnit(TimeUnit timeUnit);
	public void setTopic(String topic);
	public void setSimpleProducerConfig(SimpleProducerConfig simpleProducerConfig);
	public void setConsumerToProducerQueue(ConsumerToProducerQueue queue);
	public void setRunning(boolean isRunning);	
	public ScheduledThreadPoolExecutor getScheduledExecutor();
}
