package io.servicefabric.services.examples;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import io.servicefabric.services.ServiceFabric;
import io.servicefabric.services.ServiceNotfoundException;
import io.servicefabric.transport.protocol.Message;

/**
 * @author Anton Kharenko
 */
public class Main {

  public static void main(String[] args) {
    
	ServiceFabric fabric = new ServiceFabric(3000);
    fabric.start();
    
    
    ServiceFabric fabric1 = new ServiceFabric(3001,"localhost:3000");
    fabric1.registerService(new ExampleServiceImpl());
    fabric1.start();
    
    try {
    	
    	Map<String, String> headers = new HashMap<String, String>();
		headers.put("serviceName", "io.servicefabric.services.examples.ExampleService");
		headers.put("methodName", "sayHello");
		Message message = new Message(new HelloRequest(), headers);
		
		Thread.sleep(3000);
		fabric.invoke(message);
		
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
