package io.scalecube.services.examples;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.scalecube.transport.Message;

public class AdSelector implements AdSelectorService {

  @Override
  public ListenableFuture<Message> selectAd(Message reqMsg) {
    System.out.println("~ Received 'sayHello' RPC request: " + reqMsg);
    AdRequest reqData = reqMsg.data();
    AdResponse respData = new AdResponse("Hello " + reqData.getName());
    Message respMsg = Message.builder().data(respData).build();
    return Futures.immediateFuture(respMsg);
  }
}
