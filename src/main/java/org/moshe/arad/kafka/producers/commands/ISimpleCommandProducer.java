package org.moshe.arad.kafka.producers.commands;

import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.ICommand;
import org.moshe.arad.kafka.producers.ISimpleProducer;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;

public interface ISimpleCommandProducer <T extends ICommand> extends ISimpleProducer{

	public void sendKafkaMessage(T command);
	
	@Override
	public void setPeriod(int num);
	
	@Override
	public void setInitialDelay(int num);
	
	@Override
	public void setTimeUnit(TimeUnit timeUnit);
	
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
	
	public void setPeriodic(boolean isPeriodic);
	
	public UUID getUuid();
	
	public void setUuid(UUID uuid);
}
