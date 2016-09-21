package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import io.scalecube.transport.memoizer.Computable;
import io.scalecube.transport.memoizer.Memoizer;

import com.google.common.base.Throwables;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public final class Transport implements ITransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);
  private static final CompletableFuture<Void> COMPLETED_PROMISE = CompletableFuture.completedFuture(null);

  private final TransportConfig config;

  private final Subject<Message, Message> incomingMessagesSubject = PublishSubject.<Message>create().toSerialized();
  private final Memoizer<Address, ChannelFuture> outgoingChannels;

  // Pipeline
  private final BootstrapFactory bootstrapFactory;
  private final IncomingChannelInitializer incomingChannelInitializer = new IncomingChannelInitializer();
  private final ExceptionHandler exceptionHandler = new ExceptionHandler();
  private final MessageToByteEncoder<Message> serializerHandler;
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler;
  private final MessageReceiverHandler messageHandler;
  private final NetworkEmulatorHandler networkEmulatorHandler;

  private Address address;
  private ServerChannel serverChannel;
  private volatile boolean stopped = false;

  private Transport(TransportConfig config) {
    checkArgument(config != null);
    this.config = config;
    this.serializerHandler = new MessageSerializerHandler();
    this.deserializerHandler = new MessageDeserializerHandler();
    this.networkEmulatorHandler = config.isUseNetworkEmulator() ? new NetworkEmulatorHandler() : null;
    this.messageHandler = new MessageReceiverHandler(incomingMessagesSubject);
    this.bootstrapFactory = new BootstrapFactory(config);
    this.outgoingChannels = new Memoizer<>(new OutgoingChannelComputable());
  }

  /**
   * Init transport with the default configuration synchronously. Starts to accept connections on local address.
   */
  public static Transport bindAwait() {
    return bindAwait(TransportConfig.defaultConfig());
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
  public static CompletableFuture<Transport> bind() {
    return bind(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the given configuration asynchronously. Starts to accept connections on local address.
   */
  public static CompletableFuture<Transport> bind(TransportConfig config) {
    return new Transport(config).bind0();
  }

  /**
   * Starts to accept connections on local address.
   */
  private CompletableFuture<Transport> bind0() {
    incomingMessagesSubject.subscribeOn(Schedulers.from(bootstrapFactory.getWorkerGroup()));

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
    final CompletableFuture<Transport> result = new CompletableFuture<>();
    bindFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          serverChannel = (ServerChannel) channelFuture.channel();
          LOGGER.info("Bound to: {}", address);
          result.complete(Transport.this);
        } else {
          LOGGER.error("Failed to bind to: {}, cause: {}", address, channelFuture.cause());
          result.completeExceptionally(channelFuture.cause());
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
      LOGGER.info("Set network settings (loss={}%, mean={}ms) from {} to {}",
          lostPercent, meanDelay, address, destination);
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
      LOGGER.info("Set default network settings (loss={}%, mean={}ms)");
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
      LOGGER.info("Block network from {} to {}", address, destination);
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
      LOGGER.info("Block network from {} to {}", address, destinations);
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
      LOGGER.info("Unblock network from {} to {}", address, destination);
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
      LOGGER.info("Unblock all network from {}", address);
    } else {
      LOGGER.warn("Noop on 'unblockAll()' since network emulator is disabled");
    }
  }

  @Override
  public final void stop() {
    stop(COMPLETED_PROMISE);
  }

  @Override
  public final void stop(CompletableFuture<Void> promise) {
    checkState(!stopped, "Transport is stopped");
    checkArgument(promise != null);
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

  @Nonnull
  @Override
  public final Observable<Message> listen() {
    checkState(!stopped, "Transport is stopped");
    return incomingMessagesSubject.asObservable();
  }

  @Override
  public void send(@CheckForNull Address address, @CheckForNull Message message) {
    send(address, message, COMPLETED_PROMISE);
  }

  @Override
  public void send(@CheckForNull final Address address, @CheckForNull final Message message,
      @CheckForNull final CompletableFuture<Void> promise) {
    checkState(!stopped, "Transport is stopped");
    checkArgument(address != null);
    checkArgument(message != null);
    checkArgument(promise != null);
    message.setSender(this.address);

    final ChannelFuture channelFuture = outgoingChannels.get(address);
    if (channelFuture.isSuccess()) {
      composeFutures(channelFuture.channel().writeAndFlush(message), promise);
    } else {
      channelFuture.addListener((ChannelFuture chFuture) -> {
          if (chFuture.isSuccess()) {
            composeFutures(chFuture.channel().writeAndFlush(message), promise);
          } else {
            promise.completeExceptionally(chFuture.cause());
          }
        });
    }
  }

  /**
   * Converts netty {@link ChannelFuture} to the given  {@link CompletableFuture}.
   *
   * @param channelFuture netty channel future
   * @param promise guava future; can be null
   */
  private void composeFutures(ChannelFuture channelFuture, @Nonnull final CompletableFuture<Void> promise) {
    channelFuture.addListener((ChannelFuture future) -> {
        if (channelFuture.isSuccess()) {
          promise.complete(channelFuture.get());
        } else {
          promise.completeExceptionally(channelFuture.cause());
        }
      });
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
      if (networkEmulatorHandler != null) {
        pipeline.addLast(networkEmulatorHandler);
      }
      pipeline.addLast(exceptionHandler);
    }
  }
}
