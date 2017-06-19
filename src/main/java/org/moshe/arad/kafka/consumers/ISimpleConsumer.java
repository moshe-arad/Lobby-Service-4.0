package org.moshe.arad.kafka.consumers;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.consumers.config.SimpleConsumerConfig;

public interface ISimpleConsumer {

	public void setTopic(String topic);
	
	public void setSimpleConsumerConfig(SimpleConsumerConfig simpleConsumerConfig);
	
	public void initConsumer();
	
	public void setRunning(boolean isRunning);
	
	public ScheduledThreadPoolExecutor getScheduledExecutor();

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue);

	public void closeConsumer();
}
