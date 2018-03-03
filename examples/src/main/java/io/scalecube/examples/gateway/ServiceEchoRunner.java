package io.scalecube.examples.gateway;

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
    serverStream.listenMessageReadSuccess().subscribe(serverStream::send);

    Thread.currentThread().join();
  }
}
