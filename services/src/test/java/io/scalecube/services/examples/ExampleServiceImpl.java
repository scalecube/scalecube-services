package io.scalecube.services.examples;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.scalecube.transport.Message;

/**
 * @author Anton Kharenko
 */
public class ExampleServiceImpl implements ExampleService {

  @Override
  public ListenableFuture<Message> sayHello(Message reqMsg) {
    System.out.println("~ Received 'sayHello' RPC request: " + reqMsg);
    HelloRequest reqData = reqMsg.data();
    HelloResponse respData = new HelloResponse("Hello " + reqData.getName());
    Message respMsg = Message.builder().data(respData).build();
    return Futures.immediateFuture(respMsg);
  }
}
