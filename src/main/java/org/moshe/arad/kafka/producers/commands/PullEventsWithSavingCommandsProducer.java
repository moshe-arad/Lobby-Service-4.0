package org.moshe.arad.kafka.producers.commands;

import java.util.Date;
import java.util.UUID;

import org.moshe.arad.kafka.commands.PullEventsWithSavingCommand;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class PullEventsWithSavingCommandsProducer extends SimpleCommandsProducer<PullEventsWithSavingCommand>{

	@Autowired
	private SnapshotAPI snapshotAPI;

	@Autowired
	private ApplicationContext context;
	
	@Override
	public void doProducerCommandsOperations() {
		if(super.getUuid() == null) setUuid(UUID.randomUUID());
		PullEventsWithSavingCommand pullEventsWithSavingCommand = context.getBean(PullEventsWithSavingCommand.class);
		Date lastUpdate = snapshotAPI.getLastUpdateDate();	
		
		if(lastUpdate == null){
			pullEventsWithSavingCommand.setUuid(super.getUuid());
			pullEventsWithSavingCommand.setFromDate(new Date());
			pullEventsWithSavingCommand.setIgnoreDate(true);
			
		}
		else{
			pullEventsWithSavingCommand.setUuid(super.getUuid());
			pullEventsWithSavingCommand.setFromDate(lastUpdate);
			pullEventsWithSavingCommand.setIgnoreDate(false);
		}
		
		sendKafkaMessage(pullEventsWithSavingCommand);
	}

	public void setUuid(UUID uuid) {
		super.setUuid(uuid);
	}
}
