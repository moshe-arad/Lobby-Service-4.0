package org.moshe.arad.kafka.producers.commands;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.moshe.arad.kafka.ConsumerToProducerQueue;
import org.moshe.arad.kafka.commands.ICommand;
import org.moshe.arad.kafka.producers.config.SimpleProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * 
 * @author moshe-arad
 *
 * @param <T> is the event that we want to pass
 * 
 * important to set topic and properties before usage
 * 
 * need to set initial delay and period before executing
 */
@Component
@Scope("prototype")
public abstract class SimpleCommandsProducer <T extends ICommand> implements ISimpleCommandProducer<T>, Runnable {

	private final Logger logger = LoggerFactory.getLogger(SimpleCommandsProducer.class);
	
	@Autowired
	private SimpleProducerConfig simpleProducerConfig;
	
	private ConsumerToProducerQueue consumerToProducerQueue;
	private ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(6);
	private boolean isRunning = true;
//	private static final int PRODUCERS_NUM = 1;
	private String topic;
	
	private int initialDelay;
	private int period;
	private TimeUnit timeUnit;
	private boolean isPeriodic;
	
	private UUID uuid;
	
	public SimpleCommandsProducer() {
	}
	
	public SimpleCommandsProducer(SimpleProducerConfig simpleProducerConfig, String topic) {
		this.simpleProducerConfig = simpleProducerConfig;
		this.topic = topic;
	}
	
	@Override
    public void sendKafkaMessage(T command){
		try{
			logger.info("Users Service is about to send a Command to topic=" + topic + ", Event=" + command);
			sendMessage(command);
			logger.info("Message sent successfully, Users Service sent a Command to topic=" + topic + ", Event=" + command);
		}
		catch(Exception ex){
			logger.error("Failed to sent message, Users Service failed to send a Command to topic=" + topic + ", Event=" + command);
			logger.error(ex.getMessage());
			ex.printStackTrace();
		}
	}

	@Override
	public void run() {
		if(isPeriodic) this.takeMessagesFromConsumersAndPassPeriodic();
		else takeMessagesFromConsumersAndPass();		
	}

	private void sendMessage(T command){
		logger.info("Creating kafka producer.");
		Producer<String, String> producer = new KafkaProducer<>(simpleProducerConfig.getProperties());
		logger.info("Kafka producer created.");
		
		logger.info("Sending message to topic = " + topic + ", message = " + command.toString() + ".");
		String commandJsonBlob = convertCommandIntoJsonBlob(command);
		logger.info("Sending message to topic = " + topic + ", JSON message = " + commandJsonBlob + ".");
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, commandJsonBlob);
		producer.send(record);
		logger.info("Message sent.");
		producer.close();
		logger.info("Kafka producer closed.");
	}

	private String convertCommandIntoJsonBlob(T command){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(command);
		} catch (JsonProcessingException e) {
			logger.error("Failed to convert command into JSON blob...");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
	private void takeMessagesFromConsumersAndPassPeriodic(){
			scheduledExecutor.scheduleAtFixedRate(() -> {
					doProducerCommandsOperations();
			}, initialDelay, period, timeUnit);
	}
	
	private void takeMessagesFromConsumersAndPass(){
		doProducerCommandsOperations();
	}
	
	public abstract void doProducerCommandsOperations();
	
	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	public ScheduledThreadPoolExecutor getScheduledExecutor() {
		return scheduledExecutor;
	}

	public SimpleProducerConfig getSimpleProducerConfig() {
		return simpleProducerConfig;
	}

	public void setSimpleProducerConfig(SimpleProducerConfig simpleProducerConfig) {
		this.simpleProducerConfig = simpleProducerConfig;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public ConsumerToProducerQueue getConsumerToProducerQueue() {
		return consumerToProducerQueue;
	}

	public void setConsumerToProducerQueue(ConsumerToProducerQueue consumerToProducerQueue) {
		this.consumerToProducerQueue = consumerToProducerQueue;
	}

	public int getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(int initialDelay) {
		this.initialDelay = initialDelay;
	}

	public int getPeriod() {
		return period;
	}
	
	public void setPeriod(int period) {
		this.period = period;
	}
	
	@Override
	public void setTimeUnit(TimeUnit timeUnit) {
		this.timeUnit = timeUnit;
	}

	public boolean isPeriodic() {
		return isPeriodic;
	}

	@Override
	public void setPeriodic(boolean isPeriodic) {
		this.isPeriodic = isPeriodic;
	}

	@Override
	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
}
