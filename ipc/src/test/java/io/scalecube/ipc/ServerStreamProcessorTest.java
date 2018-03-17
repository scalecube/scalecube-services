package io.scalecube.ipc;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ServerStreamProcessorTest {

  private static final Duration TIMEOUT = Duration.ofMillis(1000);
  private static final int CONNECT_TIMEOUT_MILLIS = (int) TIMEOUT.toMillis();

  private ClientStream clientStream;
  private ListeningServerStream listeningServerStream;
  private ClientStreamProcessorFactory clientStreamProcessorFactory;
  private ServerStreamProcessorFactory serverStreamProcessorFactory;
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
    clientStreamProcessorFactory = ClientStreamProcessorFactory.newClientStreamProcessorFactory(clientStream);

    listeningServerStream = ListeningServerStream.newServerStream().withListenAddress("localhost").bind();
    address = listeningServerStream.bindAwait();
  }

  @After
  public void cleanUp() {
    clientStreamProcessorFactory.close();
    clientStream.close();
    listeningServerStream.close();
    listeningServerStream.unbindAwait();
    bootstrap.group().shutdownGracefully();
    serverStreamProcessorFactory.close();
  }

  @Test
  public void testEcho() {
    serverStreamProcessorFactory =
        ServerStreamProcessorFactory.newServerStreamProcessorFactory(listeningServerStream,
            streamProcessor -> streamProcessor.listen().subscribe(message -> {
              streamProcessor.onNext(message);
              streamProcessor.onCompleted();
            }));

    StreamProcessor clientStreamProcessor = clientStreamProcessorFactory.newClientStreamProcessor(address);
    try {
      AssertableSubscriber<ServiceMessage> clientStreamProcessorSubscriber = clientStreamProcessor.listen().test();
      clientStreamProcessor.onNext(ServiceMessage.withQualifier("q/echo").build());
      clientStreamProcessor.onCompleted();

      clientStreamProcessorSubscriber
          .awaitTerminalEventAndUnsubscribeOnTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .awaitValueCount(1, TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .assertNoErrors()
          .assertCompleted();
    } finally {
      clientStreamProcessor.close();
    }
  }
}
