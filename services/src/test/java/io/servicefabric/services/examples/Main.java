package io.servicefabric.services.examples;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import io.servicefabric.services.ServiceFabric;
import io.servicefabric.services.ServiceNotfoundException;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageHeaders;

/**
 * @author Anton Kharenko
 */
public class Main {

  public static void main(String[] args) {
    
	ServiceFabric fabric = new ServiceFabric(3000);
    fabric.start();
    
    ServiceFabric fabric1 = new ServiceFabric(3001,"localhost:3000");
    fabric1.start();
    
    ServiceFabric fabric2 = new ServiceFabric(3002,"localhost:3001");
    fabric2.start();
    
    ServiceFabric fabric3 = new ServiceFabric(3003,"localhost:3002");
    fabric3.registerService(new ExampleServiceImpl());
    fabric3.start();
    
    try {
    	
    	Map<String, String> headers = new HashMap<>();
		headers.put(MessageHeaders.SERVICE_HEADER, "io.servicefabric.services.examples.ExampleService");
		headers.put(MessageHeaders.METHOD_HEADER, "sayHello");
		
		Message message = new Message(new HelloRequest(), headers);
		for(int i=0; i<1000; i++){
			fabric1.invoke(message);
			Thread.sleep(1000);
		}
		
	} catch (InvocationTargetException | IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ServiceNotfoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

}
