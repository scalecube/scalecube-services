package io.scalecube.ipc;

import static io.scalecube.ipc.ServiceMessage.copyFrom;
import static org.junit.Assert.assertEquals;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ClientStreamProcessorTest {

  private static final Duration TIMEOUT = Duration.ofMillis(1000);
  private static final int CONNECT_TIMEOUT_MILLIS = (int) TIMEOUT.toMillis();

  private ClientStream clientStream;
  private ListeningServerStream serverStream;
  private ClientStreamProcessorFactory factory;
  private Address address;
  private Bootstrap bootstrap;

  @Before
  public void setUp() {
    bootstrap = new Bootstrap()
        .group(new NioEventLoopGroup(0))
        .channel(NioSocketChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true);

    clientStream = ClientStream.newClientStream(bootstrap);
    factory = ClientStreamProcessorFactory.newClientStreamProcessorFactory(clientStream);

    serverStream = ListeningServerStream.newServerStream().withListenAddress("localhost").bind();
    address = serverStream.listenBind().single().toBlocking().first();

    // setup echo service
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echo".equalsIgnoreCase(message.getQualifier()))
        .subscribe(message -> {
          // send original message back to client then send onCompleted
          serverStream.send(copyFrom(message).build());
          serverStream.send(copyFrom(message).qualifier(Qualifier.Q_ON_COMPLETED).build());
        });

    // setup echo service replying with void
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echoVoid".equalsIgnoreCase(message.getQualifier()))
        .subscribe(message -> {
          // just send onCompleted
          serverStream.send(copyFrom(message).qualifier(Qualifier.Q_ON_COMPLETED).build());
        });

    // setup error service
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echoError".equalsIgnoreCase(message.getQualifier()))
        .subscribe(message -> {
          // respond with error
          serverStream.send(copyFrom(message).qualifier(Qualifier.Q_GENERAL_FAILURE).build());
        });

    // setup service with several responses with onCompleted message following everyting sent
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echoStream".equalsIgnoreCase(message.getQualifier()))
        .subscribe(message -> {
          // respond with several response messages then send onCompleted
          IntStream.rangeClosed(1, 42).forEach(i -> serverStream.send(copyFrom(message).qualifier("q/" + i).build()));
          serverStream.send(copyFrom(message).qualifier(Qualifier.Q_ON_COMPLETED).build());
        });
  }

  @After
  public void cleanUp() {
    factory.close();
    clientStream.close();
    serverStream.close();
    serverStream.listenUnbind().single().toBlocking().first();
    bootstrap.group().shutdownGracefully();
  }

  @Test
  public void testEcho() throws Exception {
    ClientStreamProcessor streamProcessor = factory.newClientStreamProcessor(address);
    try {
      AssertableSubscriber<ServiceMessage> subscriber = streamProcessor.listen().test();
      streamProcessor.onNext(ServiceMessage.withQualifier("q/echo").build());
      subscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .awaitValueCount(1, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .assertCompleted();
    } finally {
      streamProcessor.close();
    }
  }

  @Test
  public void testEchoVoid() throws Exception {
    ClientStreamProcessor streamProcessor = factory.newClientStreamProcessor(address);
    try {
      AssertableSubscriber<ServiceMessage> subscriber = streamProcessor.listen().test();
      streamProcessor.onNext(ServiceMessage.withQualifier("q/echoVoid").build());
      subscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .assertCompleted()
          .assertNoValues();
    } finally {
      streamProcessor.close();
    }
  }

  @Test
  public void testEchoError() throws Exception {
    ClientStreamProcessor streamProcessor = factory.newClientStreamProcessor(address);
    try {
      AssertableSubscriber<ServiceMessage> subscriber = streamProcessor.listen().test();
      streamProcessor.onNext(ServiceMessage.withQualifier("q/echoError").build());
      subscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .assertNoValues()
          .assertError(RuntimeException.class);
    } finally {
      streamProcessor.close();
    }
  }

  @Test
  public void testEchoStream() throws Exception {
    ClientStreamProcessor streamProcessor = factory.newClientStreamProcessor(address);
    try {
      AssertableSubscriber<ServiceMessage> subscriber = streamProcessor.listen().test();
      streamProcessor.onNext(ServiceMessage.withQualifier("q/echoStream").build());
      subscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .awaitValueCount(42, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .assertCompleted();
    } finally {
      streamProcessor.close();
    }
  }

  @Test
  public void testListenFailedWhenSendFailed() throws Exception {
    Address failingAddress = Address.from("localhost:0");
    ClientStreamProcessor streamProcessor = factory.newClientStreamProcessor(failingAddress);
    try {
      AssertableSubscriber<ServiceMessage> subscriber = streamProcessor.listen().test();
      streamProcessor.onNext(ServiceMessage.withQualifier("q/echo").build());
      subscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(3, TimeUnit.SECONDS)
          .assertNoValues()
          .assertError(ConnectException.class);
    } finally {
      streamProcessor.close();
    }
  }

  @Test
  public void testListenFailedWhenRemotePartyClosed() throws Exception {
    ClientStreamProcessor streamProcessor = factory.newClientStreamProcessor(address);
    try {
      // send and receive echo message
      AssertableSubscriber<ServiceMessage> subscriber = streamProcessor.listen().test();
      streamProcessor.onNext(ServiceMessage.withQualifier("q/echo").build());
      subscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .awaitValueCount(1, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .assertCompleted();

      // close remote server stream
      AssertableSubscriber<ServiceMessage> subscriber1 = streamProcessor.listen().test();
      serverStream.close();
      Address unbindAddress = serverStream.listenUnbind().single().toBlocking().first();
      assertEquals(address, unbindAddress);

      subscriber1
          .awaitTerminalEventAndUnsubscribeOnTimeout(3, TimeUnit.SECONDS)
          .assertNoValues()
          .assertError(IOException.class);
    } finally {
      streamProcessor.close();
    }
  }
}
