package org.moshe.arad;

import org.moshe.arad.initializer.AppInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;

@SpringBootApplication
public class Application implements ApplicationRunner {

	@Autowired
	private AppInit appInit;
	
	private Logger logger = LoggerFactory.getLogger(Application.class);
	
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
	
	@Override
	public void run(ApplicationArguments arg0) throws Exception {
		appInit.startEngine();
	}
	
	@RequestMapping("/shutdown")
	public ResponseEntity<String> shutdown(){
		return doShutdown();
	}
	
	private ResponseEntity<String> doShutdown(){
		try{
			logger.info("about to do shutdown.");
			appInit.engineShutdown();
			logger.info("shutdown compeleted.");
			return new ResponseEntity<String>("", HttpStatus.OK);
		}
		catch(Exception ex){
			logger.info("Failed to shutdown users service.");
			return new ResponseEntity<String>("", HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
}
