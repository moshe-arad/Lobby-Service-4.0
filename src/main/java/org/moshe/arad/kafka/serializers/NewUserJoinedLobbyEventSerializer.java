package org.moshe.arad.kafka.serializers;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.moshe.arad.kafka.events.NewUserCreatedEvent;
import org.moshe.arad.kafka.events.NewUserJoinedLobbyEvent;

public class NewUserJoinedLobbyEventSerializer implements Serializer<NewUserJoinedLobbyEvent>{
	
	private static final String encoding = "UTF8";
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String arg0, NewUserJoinedLobbyEvent event) {
		byte[] serializedUserName;
		int sizeOfUserName;		
		byte[] serializedPassword;
		int sizeOfPassword;
		byte[] serializedFirstName;
		int sizeOfFirstName;
		byte[] serializedLastName;
		int sizeOfLastName;
		byte[] serializedEmail;
		int sizeOfEmail;
		byte[] serializedLocation;
		int sizeOfLocation;
		
		long date;
		long highUuid;
		long lowUuid;
		
		 try {
			 if (event == null)
				 return null;
            
			 serializedUserName = event.getBackgammonUser().getUserName().getBytes(encoding);
			 sizeOfUserName = event.getBackgammonUser().getUserName().length();
			 
			 serializedPassword = event.getBackgammonUser().getPassword().getBytes(encoding);
			 sizeOfPassword = event.getBackgammonUser().getPassword().length();
			 
			 serializedFirstName = event.getBackgammonUser().getFirstName().getBytes(encoding);
			 sizeOfFirstName = event.getBackgammonUser().getFirstName().length();
			 
			 serializedLastName = event.getBackgammonUser().getLastName().getBytes(encoding);
			 sizeOfLastName = event.getBackgammonUser().getLastName().length();
			 
			 serializedEmail = event.getBackgammonUser().getEmail().getBytes(encoding);
			 sizeOfEmail = event.getBackgammonUser().getEmail().length();	
			 
			 serializedLocation = event.getBackgammonUser().getLocation().name().getBytes(encoding);
			 sizeOfLocation = event.getBackgammonUser().getLocation().name().length();
			 
			 long time = event.getArrived().getTime();	
			 highUuid = event.getUuid().getMostSignificantBits();
			 lowUuid = event.getUuid().getLeastSignificantBits();
			 
			 ByteBuffer buf = ByteBuffer.allocate(sizeOfUserName+4+sizeOfPassword+4+sizeOfFirstName+4+sizeOfLastName+4+sizeOfEmail+4+sizeOfLocation+4+8+8+8);
			 buf.putInt(sizeOfUserName);
			 buf.put(serializedUserName);        
            
			 buf.putInt(sizeOfPassword);
             buf.put(serializedPassword);
             
             buf.putInt(sizeOfFirstName);
             buf.put(serializedFirstName);        
             
             buf.putInt(sizeOfLastName);
             buf.put(serializedLastName);
             
             buf.putInt(sizeOfEmail);
             buf.put(serializedEmail);
             
             buf.putInt(sizeOfLocation);
             buf.put(serializedLocation);
             
             buf.putLong(time);
             buf.putLong(highUuid);
             buf.putLong(lowUuid);
             
	         return buf.array();

	        } catch (Exception e) {
	            throw new SerializationException("Error when serializing NewUserCreatedEvent to byte[]");
	        }
	}

}
