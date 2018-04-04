package io.scalecube.streams;

import static io.scalecube.streams.StreamMessage.from;

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
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ClientStreamProcessorTest {

  private static final int CONNECT_TIMEOUT_MILLIS = (int) Duration.ofMillis(1000).toMillis();
  private static final long TIMEOUT_MILLIS = Duration.ofMillis(3000).toMillis();

  private ClientStream clientStream;
  private ListeningServerStream listeningServerStream;
  private ClientStreamProcessorFactory clientStreamProcessorFactory;
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
    clientStreamProcessorFactory = new ClientStreamProcessorFactory(clientStream);

    listeningServerStream = ListeningServerStream.newListeningServerStream().withListenAddress("localhost");
    address = listeningServerStream.bindAwait();

    // setup echo service
    listeningServerStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echo".equalsIgnoreCase(message.qualifier()))
        .subscribe(message -> {
          // send original message back to client then send onCompleted
          listeningServerStream.send(from(message).build());
          listeningServerStream.send(from(message).qualifier(Qualifier.Q_ON_COMPLETED).build());
        });

    // setup echo service replying with void
    listeningServerStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echoVoid".equalsIgnoreCase(message.qualifier()))
        .subscribe(message -> {
          // just send onCompleted
          listeningServerStream.send(from(message).qualifier(Qualifier.Q_ON_COMPLETED).build());
        });

    // setup error service
    listeningServerStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echoError".equalsIgnoreCase(message.qualifier()))
        .subscribe(message -> {
          // respond with error
          listeningServerStream.send(from(message).qualifier(Qualifier.Q_GENERAL_FAILURE).build());
        });

    // setup service with several responses with onCompleted message following everyting sent
    listeningServerStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .filter(message -> "q/echoStream".equalsIgnoreCase(message.qualifier()))
        .subscribe(message -> {
          // respond with several response messages then send onCompleted
          IntStream.rangeClosed(1, 42)
              .forEach(i -> listeningServerStream.send(from(message).qualifier("q/" + i).build()));
          listeningServerStream.send(from(message).qualifier(Qualifier.Q_ON_COMPLETED).build());
        });
  }

  @After
  public void cleanUp() {
    clientStreamProcessorFactory.close();
    clientStream.close();
    listeningServerStream.close();
    bootstrap.group().shutdownGracefully();
  }

  @Test
  public void testEcho() throws Exception {
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(address);
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echo").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .awaitValueCount(1, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertCompleted()
        .assertNoErrors();
  }

  @Test
  public void testEchoVoid() throws Exception {
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(address);
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echoVoid").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertCompleted()
        .assertNoValues()
        .assertNoErrors();
  }

  @Test
  public void testEchoError() throws Exception {
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(address);
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echoError").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoValues()
        .assertNotCompleted()
        .assertError(RuntimeException.class);
  }

  @Test
  public void testEchoStream() throws Exception {
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(address);
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echoStream").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .awaitValueCount(42, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertCompleted()
        .assertNoErrors();
  }

  @Test
  public void testListenFailedWhenSendFailedDueConnectException() {
    Address failingAddress = Address.from("localhost:0"); // host is valid port is not
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(failingAddress);
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echo").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoValues()
        .assertNotCompleted()
        .assertError(ConnectException.class);
  }

  @Test
  public void testListenFailedWhenSendFailedDueUnknownHostException() throws Exception {
    Address failingAddress = Address.from("host:0"); // invalid both host and port
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(failingAddress);
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echo").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoValues()
        .assertNotCompleted()
        .assertError(UnknownHostException.class);
  }

  @Test
  public void testListenFailedWhenRemotePartyClosed() throws Exception {
    StreamProcessor streamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(address);
    // send and receive echo message
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/echo").build());
    subscriber
        .awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .awaitValueCount(1, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertCompleted();

    // close remote server stream
    AssertableSubscriber<StreamMessage> subscriber1 = streamProcessor.listen().test();
    listeningServerStream.close();

    // wait few seconds (it's not determined how long
    // connecting party, i.e. ClientStream, should wait for signal that remote has closed socket)
    subscriber1
        .awaitTerminalEvent(3, TimeUnit.SECONDS)
        .assertNoValues()
        .assertNotCompleted()
        .assertError(IOException.class);
  }

  @Test
  public void testClientStreamChannelCloseEventsIsolated() throws Exception {
    ServerStreamProcessors server = StreamProcessors.newServer();

    // mirror events to client
    server.listen().subscribe(sp -> sp.listen().subscribe(sp));

    Address addr = server.listenAddress("localhost").bindAwait();

    ClientStreamProcessors csp1 = StreamProcessors.newClient();
    ClientStreamProcessors csp2 = StreamProcessors.newClient();

    StreamProcessor sp1 = csp1.create(addr, Object.class);
    StreamProcessor sp2 = csp2.create(addr, Object.class);

    AssertableSubscriber<StreamMessage> assertion = sp2.listen().test();

    StreamMessage req = StreamMessage.builder().qualifier("REQ").build();
    sp1.onNext(req);
    sp2.onNext(req);
    TimeUnit.SECONDS.sleep(1);
    csp1.close();
    TimeUnit.SECONDS.sleep(2);

    assertion.assertNoErrors();
  }
}
