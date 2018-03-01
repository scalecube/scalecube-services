package io.scalecube.examples.gateway;

import io.scalecube.ipc.ListeningServerStream;

public class ServiceEchoRunner {

  public static void main(String[] args) throws InterruptedException {
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().bind();
    serverStream.listenReadSuccess().subscribe(event -> serverStream.send(event.getMessage().get()));
    Thread.currentThread().join();
  }
}
