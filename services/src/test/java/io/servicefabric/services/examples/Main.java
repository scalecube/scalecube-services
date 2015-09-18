package io.servicefabric.services.examples;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import io.servicefabric.services.ServiceFabric;
import io.servicefabric.transport.protocol.Message;

/**
 * @author Anton Kharenko
 */
public class Main {

  public static void main(String[] args) {
    ServiceFabric fabric = new ServiceFabric(3000);
    
    fabric.registerService(new ExampleServiceImpl());
    
    fabric.start();
    
    try {
    	Map<String, String> headers = new HashMap<String, String>();
		headers.put("serviceName", "io.servicefabric.services.examples.ExampleService");
		headers.put("methodName", "sayHello");
		Message message = new Message(new HelloRequest(), headers);
		
		fabric.call(message);
		
	} catch (InvocationTargetException | IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

}
