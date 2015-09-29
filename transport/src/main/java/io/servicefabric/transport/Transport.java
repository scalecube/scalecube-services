package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static io.servicefabric.transport.utils.ChannelFutureUtils.setPromise;
import static io.servicefabric.transport.utils.ChannelFutureUtils.wrap;

import io.servicefabric.transport.utils.memoization.Computable;
import io.servicefabric.transport.utils.memoization.Memoizer;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Transport implements ITransportSpi, ITransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private static final Function<TransportHandshakeData, TransportEndpoint> HANDSHAKE_DATA_TO_ENDPOINT_FUNCTION =
      new Function<TransportHandshakeData, TransportEndpoint>() {
        @Override
        public TransportEndpoint apply(TransportHandshakeData handshakeData) {
          return handshakeData.endpoint();
        }
      };

  private final TransportEndpoint localEndpoint;
  private final TransportSettings settings;
  private final EventLoopGroup eventLoop;
  private final EventExecutorGroup eventExecutor;

  private final Subject<Message, Message> incomingMessagesSubject = PublishSubject.create();
  private final ConcurrentMap<TransportAddress, TransportChannel> acceptedChannels = new ConcurrentHashMap<>();
  private final Memoizer<TransportAddress, TransportChannel> connectedChannels = new Memoizer<>();

  private PipelineFactory pipelineFactory;
  private ServerChannel serverChannel;

  private Transport(TransportEndpoint localEndpoint, TransportSettings settings, EventLoopGroup eventLoop,
      EventExecutorGroup eventExecutor) {
    checkArgument(localEndpoint != null);
    checkArgument(settings != null);
    checkArgument(eventLoop != null);
    checkArgument(eventExecutor != null);
    this.localEndpoint = localEndpoint;
    this.settings = settings;
    this.eventLoop = eventLoop;
    this.eventExecutor = eventExecutor;
    this.pipelineFactory =
        new TransportPipelineFactory(this, new ProtostuffProtocol(), settings.isUseNetworkEmulator());
  }

  public static Transport newInstance(TransportEndpoint localEndpoint) {
    return newInstance(localEndpoint, TransportSettings.DEFAULT);
  }

  public static Transport newInstance(TransportEndpoint localEndpoint, TransportSettings settings) {
    return newInstance(localEndpoint, settings, defaultEventLoop(localEndpoint), defaultEventExecutor(localEndpoint));
  }

  public static Transport newInstance(TransportEndpoint localEndpoint, EventLoopGroup eventLoop,
      EventExecutorGroup eventExecutor) {
    return newInstance(localEndpoint, TransportSettings.DEFAULT, eventLoop, eventExecutor);
  }

  public static Transport newInstance(TransportEndpoint localEndpoint, TransportSettings settings,
      EventLoopGroup eventLoop, EventExecutorGroup eventExecutor) {
    return new Transport(localEndpoint, settings, eventLoop, eventExecutor);
  }

  private static EventLoopGroup defaultEventLoop(TransportEndpoint localEndpoint) {
    ThreadFactory eventLoopThreadFactory = createThreadFactory("servicefabric-transport-io-%s@" + localEndpoint);
    return new NioEventLoopGroup(1, eventLoopThreadFactory);
  }

  private static EventExecutorGroup defaultEventExecutor(TransportEndpoint localEndpoint) {
    ThreadFactory eventExecutorThreadFactory = createThreadFactory("servicefabric-transport-exec-%s@" + localEndpoint);
    return new DefaultEventExecutorGroup(1, eventExecutorThreadFactory);
  }

  private static ThreadFactory createThreadFactory(String namingFormat) {
    return new ThreadFactoryBuilder().setNameFormat(namingFormat)
        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable ex) {
            LOGGER.error("Unhandled exception: {}", ex, ex);
          }
        }).setDaemon(true).build();
  }

  @Override
  public TransportEndpoint localEndpoint() {
    return localEndpoint;
  }

  public EventLoopGroup getEventLoop() {
    return eventLoop;
  }

  @Override
  public EventExecutorGroup getEventExecutor() {
    return eventExecutor;
  }

  @Override
  public final int getHandshakeTimeout() {
    return settings.getHandshakeTimeout();
  }

  @Override
  public int getSendHighWaterMark() {
    return settings.getSendHighWaterMark();
  }

  @Override
  public LogLevel getLogLevel() {
    String logLevel = settings.getLogLevel();
    if (logLevel != null && !logLevel.equals("OFF")) {
      return LogLevel.valueOf(logLevel);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public <T extends PipelineFactory> T getPipelineFactory() {
    return (T) pipelineFactory;
  }

  @Override
  public final void start() {
    incomingMessagesSubject.subscribeOn(Schedulers.from(eventExecutor)); // define that we making smart subscribe

    Class<? extends ServerChannel> serverChannelClass = NioServerSocketChannel.class;
    SocketAddress bindAddress = new InetSocketAddress(localEndpoint.address().port());

    ServerBootstrap server = new ServerBootstrap();
    server.group(eventLoop).channel(serverChannelClass).childHandler(new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel channel) {
        pipelineFactory.setAcceptorPipeline(channel, Transport.this);
      }
    });
    try {
      serverChannel = (ServerChannel) server.bind(bindAddress).syncUninterruptibly().channel();
      LOGGER.info("Transport endpoint '{}' bound to: {}", localEndpoint.id(), bindAddress);
    } catch (Exception e) {
      LOGGER.error("Failed to bind to: " + bindAddress + ", caught " + e, e);
      propagate(e);
    }
  }

  @Override
  public ListenableFuture<TransportEndpoint> connect(@CheckForNull final TransportAddress address) {
    checkArgument(address != null);
    final TransportChannel transportChannel = getOrConnect(address);
    return Futures.transform(transportChannel.handshakeFuture(), HANDSHAKE_DATA_TO_ENDPOINT_FUNCTION);
  }

  private void connect(final Channel channel, final TransportAddress address, final TransportChannel transport) {
    channel.eventLoop().execute(new Runnable() {
      @Override
      public void run() {
        SocketAddress socketAddress = new InetSocketAddress(address.hostAddress(), address.port());
        ChannelPromise promise = channel.newPromise();
        channel.connect(socketAddress, promise);
        promise.addListener(wrap(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) {
            if (!future.isSuccess()) {
              transport.close(future.cause());
            }
          }
        }));
      }
    });
  }

  @Override
  public void disconnect(@CheckForNull TransportEndpoint endpoint, @Nullable SettableFuture<Void> promise) {
    checkArgument(endpoint != null);
    TransportChannel transportChannel = connectedChannels.getIfExists(endpoint.address());
    // TODO [AK]: check that channel endpoint id correspond to provided endpoint id; fail otherwise
    if (transportChannel == null) {
      if (promise != null) {
        promise.set(null);
      }
    } else {
      transportChannel.close(promise);
    }
  }

  @Override
  public void send(@CheckForNull TransportEndpoint endpoint, @CheckForNull Message message) {
    send(endpoint, message, null);
  }

  @Override
  public void send(@CheckForNull TransportEndpoint endpoint, @CheckForNull Message message,
      @Nullable SettableFuture<Void> promise) {
    checkArgument(endpoint != null);
    checkArgument(message != null);
    TransportChannel transportChannel = getOrConnect(endpoint.address());
    // TODO [AK]: check that channel endpoint id correspond to provided endpoint id; fail otherwise
    transportChannel.send(message, promise);
  }

  @Nonnull
  @Override
  public final Observable<Message> listen() {
    return incomingMessagesSubject;
  }

  @Override
  public final void stop() {
    stop(null);
  }

  @Override
  public final void stop(@Nullable SettableFuture<Void> promise) {
    try {
      incomingMessagesSubject.onCompleted();
    } catch (Exception ignore) {
      // ignore
    }
    // cleanup accepted
    for (TransportAddress address : acceptedChannels.keySet()) {
      TransportChannel transport = acceptedChannels.remove(address);
      if (transport != null) {
        transport.close();
      }
    }
    // cleanup connected
    for (TransportAddress address : connectedChannels.keySet()) {
      TransportChannel transport = connectedChannels.remove(address);
      if (transport != null) {
        transport.close();
      }
    }
    if (serverChannel != null) {
      setPromise(serverChannel.close(), promise);
    }
  }

  @Override
  public TransportChannel createAcceptorTransportChannel(Channel channel) {
    return TransportChannel.newAcceptorChannel(channel, new Func1<TransportChannel, Void>() {
      @Override
      public Void call(TransportChannel transportChannel) {
        TransportEndpoint remoteEndpoint = transportChannel.remoteEndpoint();
        if (remoteEndpoint != null) {
          acceptedChannels.remove(remoteEndpoint.address());
        }
        return null;
      }
    });
  }

  @Override
  public void accept(TransportChannel transportChannel) throws TransportBrokenException {
    TransportEndpoint remoteEndpoint = transportChannel.remoteEndpoint();
    checkNotNull(remoteEndpoint);
    checkNotNull(remoteEndpoint.address());
    TransportChannel prev = acceptedChannels.putIfAbsent(remoteEndpoint.address(), transportChannel);
    if (prev != null) {
      String err = String.format("Detected duplicate %s for key=%s in accepted_map", prev, remoteEndpoint);
      throw new TransportBrokenException(err);
    }
  }

  @Override
  public void resetDueHandshake(Channel channel) {
    pipelineFactory.resetDueHandshake(channel, this);
  }

  @Override
  public void onMessage(Message message) {
    incomingMessagesSubject.onNext(message);
  }

  private TransportChannel getOrConnect(@CheckForNull final TransportAddress address) {
    checkArgument(address != null);
    return connectedChannels.get(address, new Computable<TransportAddress, TransportChannel>() {
      @Override
      public TransportChannel compute(final TransportAddress address) {
        final Channel channel = createConnectorChannel();
        final TransportChannel transportChannel = createConnectorTransportChannel(channel, address);

        LOGGER.info("Registered connector: {}", transportChannel);

        final ChannelFuture registerChannelFuture = eventLoop.register(channel);
        registerChannelFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              connect(channel, address, transportChannel);
            } else {
              channel.unsafe().closeForcibly();
              transportChannel.close();
            }
          }
        });
        return transportChannel;
      }
    });
  }

  private Channel createConnectorChannel() {
    Channel channel = new NioSocketChannel();
    pipelineFactory.setConnectorPipeline(channel, this);
    channel.config().setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.getConnectTimeout());
    channel.config().setOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    channel.config().setOption(ChannelOption.TCP_NODELAY, true);
    channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
    channel.config().setOption(ChannelOption.SO_REUSEADDR, true);
    return channel;
  }

  private TransportChannel createConnectorTransportChannel(Channel channel, final TransportAddress endpoint) {
    return TransportChannel.newConnectorChannel(channel, new Func1<TransportChannel, Void>() {
      @Override
      public Void call(TransportChannel transport) {
        connectedChannels.remove(endpoint);
        return null;
      }
    });
  }


}
