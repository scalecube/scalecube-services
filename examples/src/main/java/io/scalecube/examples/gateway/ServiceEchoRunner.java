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
    ListeningServerStream listeningServerStream = ListeningServerStream.newListeningServerStream();
    listeningServerStream.bindAwait();
    listeningServerStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(listeningServerStream::send);

    Thread.currentThread().join();
  }
}
