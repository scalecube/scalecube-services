package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

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

import java.net.BindException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public final class Transport implements ITransport {
  private static long DEFAULT_BUFFER_LIMIT = 5000;
  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);
  private static final CompletableFuture<Void> COMPLETED_PROMISE = CompletableFuture.completedFuture(null);

  private final TransportConfig config;

  private final Subject<Message, Message> incomingMessagesSubject = PublishSubject.<Message>create().toSerialized();

  private final Map<Address, ChannelFuture> outgoingChannels = new ConcurrentHashMap<>();

  // Pipeline
  private final BootstrapFactory bootstrapFactory;
  private final IncomingChannelInitializer incomingChannelInitializer = new IncomingChannelInitializer();
  private final ExceptionHandler exceptionHandler = new ExceptionHandler();
  private final MessageToByteEncoder<Message> serializerHandler;
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler;
  private final MessageHandler messageHandler;

  // Network emulator
  private NetworkEmulator networkEmulator;
  private NetworkEmulatorHandler networkEmulatorHandler;

  private Address address;
  private ServerChannel serverChannel;

  private volatile boolean stopped = false;

  private Transport(TransportConfig config) {
    checkArgument(config != null);
    this.config = config;
    this.serializerHandler = new MessageSerializerHandler();
    this.deserializerHandler = new MessageDeserializerHandler();
    this.messageHandler = new MessageHandler(incomingMessagesSubject);
    this.bootstrapFactory = new BootstrapFactory(config);
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
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        serverChannel = (ServerChannel) channelFuture.channel();
        networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
        networkEmulatorHandler = config.isUseNetworkEmulator() ? new NetworkEmulatorHandler(networkEmulator) : null;
        LOGGER.info("Bound to: {}", address);
        result.complete(Transport.this);
      } else {
        Throwable cause = channelFuture.cause();
        if (config.isPortAutoIncrement() && isAddressAlreadyInUseException(cause)) {
          LOGGER.warn("Can't bind to address {}, try again on different port [cause={}]", address, cause.toString());
          bind0().thenAccept(result::complete);
        } else {
          LOGGER.error("Failed to bind to: {}, cause: {}", address, cause);
          result.completeExceptionally(cause);
        }
      }
    });
    return result;
  }

  private boolean isAddressAlreadyInUseException(Throwable exception) {
    return exception instanceof BindException
        || (exception.getMessage() != null && exception.getMessage().contains("Address already in use"));
  }

  @Override
  @Nonnull
  public Address address() {
    return address;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Nonnull
  @Override
  public NetworkEmulator networkEmulator() {
    return networkEmulator;
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
      ChannelFuture channelFuture = outgoingChannels.get(address);
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
    return incomingMessagesSubject.onBackpressureBuffer(DEFAULT_BUFFER_LIMIT).asObservable();
  }

  @Override
  public void send(@CheckForNull Address address, @CheckForNull Message message) {
    send(address, message, COMPLETED_PROMISE);
  }

  @Override
  public void send(@CheckForNull Address address, @CheckForNull Message message,
      @CheckForNull CompletableFuture<Void> promise) {
    checkState(!stopped, "Transport is stopped");
    checkArgument(address != null);
    checkArgument(message != null);
    checkArgument(promise != null);
    message.setSender(this.address);

    final ChannelFuture channelFuture = outgoingChannels.computeIfAbsent(address, this::connect);
    if (channelFuture.isSuccess()) {
      send(channelFuture.channel(), message, promise);
    } else {
      channelFuture.addListener((ChannelFuture chFuture) -> {
        if (chFuture.isSuccess()) {
          send(channelFuture.channel(), message, promise);
        } else {
          promise.completeExceptionally(chFuture.cause());
        }
      });
    }
  }

  private void send(Channel channel, Message message, CompletableFuture<Void> promise) {
    if (promise == COMPLETED_PROMISE) {
      channel.writeAndFlush(message, channel.voidPromise());
    } else {
      composeFutures(channel.writeAndFlush(message), promise);
    }
  }

  /**
   * Converts netty {@link ChannelFuture} to the given {@link CompletableFuture}.
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

  private ChannelFuture connect(Address address) {
    OutgoingChannelInitializer channelInitializer = new OutgoingChannelInitializer(address);
    Bootstrap client = bootstrapFactory.clientBootstrap().handler(channelInitializer);
    ChannelFuture connectFuture = client.connect(address.host(), address.port());

    // Register logger and cleanup listener
    connectFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        LOGGER.debug("Connected from {} to {}: {}", Transport.this.address, address, channelFuture.channel());
      } else {
        LOGGER.warn("Failed to connect from {} to {}", Transport.this.address, address);
        outgoingChannels.remove(address);
      }
    });

    return connectFuture;
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
          outgoingChannels.remove(address);
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
