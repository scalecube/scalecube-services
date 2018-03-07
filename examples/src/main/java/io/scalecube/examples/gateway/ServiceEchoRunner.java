package io.scalecube.examples.gateway;

import io.scalecube.ipc.Event;
import io.scalecube.ipc.ListeningServerStream;

/**
 * Simple echo runner.
 */
public class ServiceEchoRunner {

  /**
   * Main method.
   */
  public static void main(String[] args) throws InterruptedException {
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().bind();
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);

    Thread.currentThread().join();
  }
}
