package io.scalecube.examples.gateway;

import io.scalecube.streams.Event;
import io.scalecube.streams.ListeningServerStream;

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
