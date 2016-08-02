package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.transport.memoizer.Computable;
import io.scalecube.transport.memoizer.Memoizer;
import io.scalecube.transport.utils.FutureUtils;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.FutureFallback;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
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
  private final ConcurrentMap<InetSocketAddress, TransportChannel> acceptedChannels = new ConcurrentHashMap<>();
  private final Memoizer<InetSocketAddress, TransportChannel> connectedChannels = new Memoizer<>();

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
    ThreadFactory eventLoopThreadFactory = createThreadFactory("scalecube-transport-io-%s@" + localEndpoint);
    return new NioEventLoopGroup(1, eventLoopThreadFactory);
  }

  private static EventExecutorGroup defaultEventExecutor(TransportEndpoint localEndpoint) {
    ThreadFactory eventExecutorThreadFactory = createThreadFactory("scalecube-transport-exec-%s@" + localEndpoint);
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
  public final ListenableFuture<Void> start() {
    incomingMessagesSubject.subscribeOn(Schedulers.from(eventExecutor)); // define that we making smart subscribe

    Class<? extends ServerChannel> serverChannelClass = NioServerSocketChannel.class;

    ServerBootstrap server = new ServerBootstrap();
    server.group(eventLoop).channel(serverChannelClass).childHandler(new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel channel) {
        pipelineFactory.setAcceptorPipeline(channel, Transport.this);
      }
    });

    ChannelFuture bindFuture = server.bind(localEndpoint.port());
    final SettableFuture<Void> result = SettableFuture.create();
    bindFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          serverChannel = (ServerChannel) channelFuture.channel();
          LOGGER.info("Bound transport endpoint: {} at {}:{}",
              localEndpoint.id(), localEndpoint.host(), localEndpoint.port());
          result.set(null);
        } else {
          LOGGER.error("Failed to bind transport endpoint: {} at {}:{}, cause: {}",
              localEndpoint.id(), localEndpoint.host(), localEndpoint.port(), channelFuture.cause());
          result.setException(channelFuture.cause());
        }
      }
    });
    return result;
  }

  @Override
  public ListenableFuture<TransportEndpoint> connect(@CheckForNull InetSocketAddress address) {
    checkArgument(address != null);
    return Futures.transform(getOrConnect(address), new AsyncFunction<TransportChannel, TransportEndpoint>() {
      @Override
      public ListenableFuture<TransportEndpoint> apply(TransportChannel input) {
        return Futures.transform(input.handshakeFuture(), HANDSHAKE_DATA_TO_ENDPOINT_FUNCTION);
      }
    });
  }

  private void connect(final Channel channel, final InetSocketAddress address, final TransportChannel transport) {
    channel.eventLoop().execute(new Runnable() {
      @Override
      public void run() {
        ChannelPromise promise = channel.newPromise();
        channel.connect(address, promise);
        promise.addListener(FutureUtils.wrap(new ChannelFutureListener() {
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
    TransportChannel transportChannel = null;
    for (InetSocketAddress address : connectedChannels.keySet()) {
      if (address.getAddress().getHostAddress().equals(endpoint.host()) && address.getPort() == endpoint.port()) {
        transportChannel = connectedChannels.get(address);
        break;
      }
    }
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
  public void send(@CheckForNull final TransportEndpoint endpoint, @CheckForNull final Message message,
      @Nullable final SettableFuture<Void> promise) {
    checkArgument(endpoint != null);
    checkArgument(message != null);
    Futures.addCallback(getOrConnect(endpoint.socketAddress()), new FutureCallback<TransportChannel>() {
      @Override
      public void onSuccess(TransportChannel input) {
        input.send(message, promise);
      }

      @Override
      public void onFailure(Throwable cause) {
        LOGGER.error("Failed to get transportChannel by socketAddress: {}, cause: {}", endpoint.socketAddress(), cause);
        if (promise != null) {
          promise.setException(cause);
        }
      }
    });
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
    for (InetSocketAddress address : acceptedChannels.keySet()) {
      TransportChannel transport = acceptedChannels.remove(address);
      if (transport != null) {
        transport.close();
      }
    }
    // cleanup connected
    for (InetSocketAddress address : connectedChannels.keySet()) {
      TransportChannel transport = connectedChannels.remove(address);
      if (transport != null) {
        transport.close();
      }
    }
    if (serverChannel != null) {
      FutureUtils.compose(serverChannel.close(), promise);
    }
  }

  @Override
  public TransportChannel createAcceptorTransportChannel(Channel channel) {
    return TransportChannel.newAcceptorChannel(channel, new Func1<TransportChannel, Void>() {
      @Override
      public Void call(TransportChannel transportChannel) {
        TransportEndpoint remoteEndpoint = transportChannel.remoteEndpoint();
        if (remoteEndpoint != null) {
          acceptedChannels.remove(remoteEndpoint.socketAddress());
        }
        return null;
      }
    });
  }

  @Override
  public void accept(TransportChannel transportChannel) throws TransportBrokenException {
    TransportEndpoint remoteEndpoint = transportChannel.remoteEndpoint();
    checkNotNull(remoteEndpoint);
    checkNotNull(remoteEndpoint.socketAddress());
    TransportChannel prev = acceptedChannels.putIfAbsent(remoteEndpoint.socketAddress(), transportChannel);
    if (prev != null) {
      throw new TransportBrokenException(
          String.format("Detected duplicate %s for key=%s in accepted_map", prev, remoteEndpoint));
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

  private ListenableFuture<TransportChannel> getOrConnect(@CheckForNull InetSocketAddress address) {
    checkArgument(address != null);
    return Futures.transform(resolveSocketAddress(address), new Function<InetSocketAddress, TransportChannel>() {
      @Override
      public TransportChannel apply(final InetSocketAddress resolvedAddress) {
        return connectedChannels.get(resolvedAddress, new Computable<InetSocketAddress, TransportChannel>() {
          @Override
          public TransportChannel compute(final InetSocketAddress input) {
            final Channel channel = createConnectorChannel();
            final TransportChannel transportChannel = createConnectorTransportChannel(channel, input);

            Futures.addCallback(registerChannel(channel), new FutureCallback<Void>() {
              @Override
              public void onSuccess(Void void0) {
                connect(channel, input, transportChannel);
              }

              @Override
              public void onFailure(Throwable throwable) {
                channel.unsafe().closeForcibly();
                transportChannel.close();
              }
            });

            return transportChannel;
          }
        });
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

  private TransportChannel createConnectorTransportChannel(Channel channel, final InetSocketAddress address) {
    return TransportChannel.newConnectorChannel(channel, new Func1<TransportChannel, Void>() {
      @Override
      public Void call(TransportChannel transport) {
        connectedChannels.remove(address);
        return null;
      }
    });
  }

  /**
   * Asynchronously registers channel in {@link #eventLoop}.
   */
  private ListenableFuture<Void> registerChannel(Channel channel) {
    return Futures.withFallback(FutureUtils.compose(eventLoop.register(channel)), new FutureFallback<Void>() {
      @Override
      public ListenableFuture<Void> create(Throwable cause) {
        LOGGER.error("Failed to register channel, cause: {}", new Object[] {cause});
        return Futures.immediateFailedFuture(cause);
      }
    });
  }

  /**
   * Asynchronously resolves inetAddress by hostname taken from socketAddress ({@link InetSocketAddress#getHostName()}).
   * 
   * @return new {@link InetSocketAddress} object with resolved {@link InetAddress}.
   */
  private ListenableFuture<InetSocketAddress> resolveSocketAddress(final InetSocketAddress address) {
    return Futures.withFallback(FutureUtils.compose(eventLoop.submit(new Callable<InetSocketAddress>() {
      @Override
      public InetSocketAddress call() throws Exception {
        InetAddress inetAddress = InetAddress.getByName(address.getHostName()); // issue reverse look up here
        return new InetSocketAddress(inetAddress, address.getPort());
      }
    })), new FutureFallback<InetSocketAddress>() {
      @Override
      public ListenableFuture<InetSocketAddress> create(Throwable cause) throws Exception {
        LOGGER.error("Failed to resolve inet address by hostname: {}, cause: {}", address.getHostName(), cause);
        return Futures.immediateFailedFuture(cause);
      }
    });
  }
}
