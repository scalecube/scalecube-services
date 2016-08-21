package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import io.scalecube.transport.memoizer.Computable;
import io.scalecube.transport.memoizer.Memoizer;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.Executor;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

public final class Transport implements ITransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private final TransportConfig config;

  private final Subject<Message, Message> incomingMessagesSubject = PublishSubject.create();
  private final Memoizer<Address, ChannelFuture> outgoingChannels;

  // Pipeline
  private final BootstrapFactory bootstrapFactory;
  private final IncomingChannelInitializer incomingChannelInitializer = new IncomingChannelInitializer();
  private final ExceptionHandler exceptionHandler = new ExceptionHandler();
  private final MessageToByteEncoder<Message> serializerHandler;
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler;
  private final MessageReceiverHandler messageHandler;
  private final LoggingHandler loggingHandler;
  private final NetworkEmulatorHandler networkEmulatorHandler;

  private Address address;
  private ServerChannel serverChannel;
  private volatile boolean stopped = false;

  private Transport(TransportConfig config) {
    checkArgument(config != null);
    this.config = config;
    this.serializerHandler = new MessageSerializerHandler();
    this.deserializerHandler = new MessageDeserializerHandler();
    LogLevel logLevel = resolveLogLevel(config.getLogLevel());
    this.loggingHandler = logLevel != null ? new LoggingHandler(logLevel) : null;
    this.networkEmulatorHandler = config.isUseNetworkEmulator() ? new NetworkEmulatorHandler() : null;
    this.messageHandler = new MessageReceiverHandler(incomingMessagesSubject);
    this.bootstrapFactory = new BootstrapFactory(config);
    this.outgoingChannels = new Memoizer<>(new OutgoingChannelComputable());
  }

  private LogLevel resolveLogLevel(String logLevel) {
    return (logLevel != null && !logLevel.equals("OFF")) ? LogLevel.valueOf(logLevel) : null;
  }

  /**
   * Init transport with the default configuration synchronously. Starts to accept connections on local address.
   */
  public static Transport bindAwait() {
    return bindAwait(TransportConfig.DEFAULT);
  }

  /**
   * Init transport with the default configuration and network emulator flag synchronously. Starts to accept connections
   * on local address.
   */
  public static Transport bindAwait(boolean useNetworkEmulator) {
    return bindAwait(TransportConfig.builder().useNetworkEmulator(useNetworkEmulator).build());
  }

  /**
   * Init transport with the given configuration synchronously. Starts to accept connections on local address.
   */
  public static Transport bindAwait(TransportConfig config) {
    try {
      return bind(config).get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  /**
   * Init transport with the default configuration asynchronously. Starts to accept connections on local address.
   */
  public static ListenableFuture<Transport> bind() {
    return bind(TransportConfig.DEFAULT);
  }

  /**
   * Init transport with the given configuration asynchronously. Starts to accept connections on local address.
   */
  public static ListenableFuture<Transport> bind(TransportConfig config) {
    return new Transport(config).bind0();
  }

  /**
   * Starts to accept connections on local address.
   */
  private ListenableFuture<Transport> bind0() {
    // Resolve listen IP address
    final InetAddress listenAddress =
        Addressing.getLocalIpAddress(config.getListenAddress(), config.getListenInterface(), config.isPreferIPv6());

    // Resolve listen port
    int bindPort = config.isPortAutoIncrement()
        ? Addressing.getNextAvailablePort(listenAddress, config.getPort(), config.getPortCount()) // Find available port
        : config.getPort();

    // Listen address
    address = Address.create(listenAddress.getHostAddress(), bindPort);

    ServerBootstrap server = bootstrapFactory.serverBootstrap().childHandler(incomingChannelInitializer);
    ChannelFuture bindFuture = server.bind(listenAddress, address.port());
    final SettableFuture<Transport> result = SettableFuture.create();
    bindFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          serverChannel = (ServerChannel) channelFuture.channel();
          LOGGER.info("Bound to: {}", listenAddress);
          result.set(Transport.this);
        } else {
          LOGGER.error("Failed to bind to: {}, cause: {}", listenAddress, channelFuture.cause());
          result.setException(channelFuture.cause());
        }
      }
    });
    return result;
  }

  @Override
  public Address address() {
    return address;
  }

  public boolean isStopped() {
    return stopped;
  }

  /**
   * Sets given network emulator settings. If network emulator is disabled do nothing.
   */
  public void setNetworkSettings(Address destination, int lostPercent, int meanDelay) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.setNetworkSettings(destination, lostPercent, meanDelay);
    } else {
      LOGGER.warn("Noop on 'setNetworkSettings({},{},{})' since network emulator is disabled",
          destination, lostPercent, meanDelay);
    }
  }

  /**
   * Sets default network emulator settings. If network emulator is disabled do nothing.
   */
  public void setDefaultNetworkSettings(int lostPercent, int meanDelay) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.setDefaultNetworkSettings(lostPercent, meanDelay);
    } else {
      LOGGER.warn("Noop on 'setDefaultNetworkSettings({},{})' since network emulator is disabled",
          lostPercent, meanDelay);
    }
  }

  /**
   * Block messages to given destination. If network emulator is disabled do nothing.
   */
  public void block(Address destination) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.block(destination);
    } else {
      LOGGER.warn("Noop on 'block({})' since network emulator is disabled", destination);
    }
  }

  /**
   * Block messages to the given destinations. If network emulator is disabled do nothing.
   */
  public void block(Collection<Address> destinations) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.block(destinations);
    } else {
      LOGGER.warn("Noop on 'block({})' since network emulator is disabled", destinations);
    }
  }

  /**
   * Unblock messages to given destination. If network emulator is disabled do nothing.
   */
  public void unblock(Address destination) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.unblock(destination);
    } else {
      LOGGER.warn("Noop on 'unblock({})' since network emulator is disabled", destination);
    }
  }

  /**
   * Unblock messages to all destinations. If network emulator is disabled do nothing.
   */
  public void unblockAll() {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.unblockAll();
    } else {
      LOGGER.warn("Noop on 'unblockAll()' since network emulator is disabled");
    }
  }

  @Override
  public final void stop() {
    stop(null);
  }

  @Override
  public final void stop(@Nullable SettableFuture<Void> promise) {
    checkState(!stopped, "Transport is stopped");
    stopped = true;
    // Complete incoming messages observable
    try {
      incomingMessagesSubject.onCompleted();
    } catch (Exception ignore) {
      // ignore
    }

    // close connected channels
    for (Address address : outgoingChannels.keySet()) {
      ChannelFuture channelFuture = outgoingChannels.getIfExists(address);
      if (channelFuture == null) {
        continue;
      }
      if (channelFuture.isSuccess()) {
        channelFuture.channel().close();
      } else {
        channelFuture.addListener(ChannelFutureListener.CLOSE);
      }
    }
    outgoingChannels.clear();

    // close server channel
    if (serverChannel != null) {
      composeFutures(serverChannel.close(), promise);
    }

    // TODO [AK]: shutdown boss/worker threads and listen for their futures
    bootstrapFactory.shutdown();
  }

  @Override
  public Observable<Message> listen() {
    checkState(!stopped, "Transport is stopped");
    return incomingMessagesSubject;
  }

  @Override
  public Observable<Message> listen(Executor executor) {
    return listen(Schedulers.from(executor));
  }

  @Override
  public Observable<Message> listen(Scheduler scheduler) {
    return listen().observeOn(scheduler);
  }

  @Override
  public void send(@CheckForNull Address address, @CheckForNull Message message) {
    send(address, message, null);
  }

  @Override
  public void send(@CheckForNull final Address address, @CheckForNull final Message message,
      @Nullable final SettableFuture<Void> promise) {
    checkState(!stopped, "Transport is stopped");
    checkArgument(address != null);
    checkArgument(message != null);
    message.setSender(this.address);

    final ChannelFuture channelFuture = outgoingChannels.get(address);
    if (channelFuture.isSuccess()) {
      composeFutures(channelFuture.channel().writeAndFlush(message), promise);
    } else {
      channelFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) {
          if (channelFuture.isSuccess()) {
            composeFutures(channelFuture.channel().writeAndFlush(message), promise);
          } else {
            if (promise != null) {
              promise.setException(channelFuture.cause());
            }
          }
        }
      });
    }
  }

  /**
   * Converts netty {@link ChannelFuture} to the given guava {@link SettableFuture}.
   *
   * @param channelFuture netty channel future
   * @param promise guava future; can be null
   */
  private void composeFutures(ChannelFuture channelFuture, @Nullable final SettableFuture<Void> promise) {
    if (promise != null) {
      channelFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          if (channelFuture.isSuccess()) {
            promise.set(channelFuture.get());
          } else {
            promise.setException(channelFuture.cause());
          }
        }
      });
    }
  }

  private final class OutgoingChannelComputable implements Computable<Address, ChannelFuture> {
    @Override
    public ChannelFuture compute(final Address address) throws Exception {
      OutgoingChannelInitializer channelInitializer = new OutgoingChannelInitializer(address);
      Bootstrap client = bootstrapFactory.clientBootstrap().handler(channelInitializer);
      ChannelFuture connectFuture = client.connect(address.host(), address.port());

      // Register logger and cleanup listener
      connectFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) {
          if (channelFuture.isSuccess()) {
            LOGGER.info("Connected from {} to {}: {}", Transport.this.address, address, channelFuture.channel());
          } else {
            LOGGER.warn("Failed to connect from {} to {}", Transport.this.address, address);
            outgoingChannels.delete(address);
          }
        }
      });

      return connectFuture;
    }
  }

  @ChannelHandler.Sharable
  private final class IncomingChannelInitializer extends ChannelInitializer {
    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ProtobufVarint32FrameDecoder());
      pipeline.addLast(deserializerHandler);
      if (loggingHandler != null) {
        pipeline.addLast(loggingHandler);
      }
      pipeline.addLast(messageHandler);
      pipeline.addLast(exceptionHandler);
    }
  }

  @ChannelHandler.Sharable
  private final class OutgoingChannelInitializer extends ChannelInitializer {
    private final Address address;

    public OutgoingChannelInitializer(Address address) {
      this.address = address;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ChannelDuplexHandler() {
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
          LOGGER.debug("Disconnected from: {} {}", address, ctx.channel());
          outgoingChannels.delete(address);
          super.channelInactive(ctx);
        }
      });
      pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
      pipeline.addLast(serializerHandler);
      if (loggingHandler != null) {
        pipeline.addLast(loggingHandler);
      }
      if (networkEmulatorHandler != null) {
        pipeline.addLast(networkEmulatorHandler);
      }
      pipeline.addLast(exceptionHandler);
    }
  }
}
