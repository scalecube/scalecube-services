package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.memoizer.Computable;
import io.scalecube.transport.memoizer.Memoizer;
import io.scalecube.transport.utils.FutureUtils;

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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Transport implements ITransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private final TransportEndpoint localEndpoint;

  private final Subject<Message, Message> incomingMessagesSubject = PublishSubject.create();
  private final Memoizer<TransportEndpoint, ChannelFuture> outgoingChannels;

  // TODO: move into network emulator handler
  private final Map<TransportEndpoint, NetworkEmulatorSettings> networkEmulatorSettings = new ConcurrentHashMap<>();

  // Shared handlers
  private final Protocol protocol;
  private final ExceptionCaughtChannelHandler exceptionHandler = new ExceptionCaughtChannelHandler();
  private final MessageToByteEncoder<Message> serializerHandler;
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler;
  private final LoggingHandler loggingHandler;
  private final NetworkEmulatorChannelHandler networkEmulatorHandler;
  private final MessageReceiverChannelHandler messageHandler;

  // Channel initializers
  private final NettyBootstrapFactory bootstrapFactory;
  private final IncomingChannelInitializer incomingChannelInitializer = new IncomingChannelInitializer();

  private ServerChannel serverChannel;

  private Transport(TransportEndpoint localEndpoint, TransportSettings settings) {
    checkArgument(localEndpoint != null);
    checkArgument(settings != null);
    this.localEndpoint = localEndpoint;
    this.protocol = new ProtostuffProtocol();
    this.serializerHandler = new SharableSerializerHandler(protocol.getMessageSerializer());
    this.deserializerHandler = new SharableDeserializerHandler(protocol.getMessageDeserializer());
    LogLevel logLevel = resolveLogLevel(settings.getLogLevel());
    this.loggingHandler = logLevel != null ? new LoggingHandler(logLevel) : null;
    boolean useNetworkEmulator = settings.isUseNetworkEmulator();
    this.networkEmulatorHandler =
        useNetworkEmulator ? new NetworkEmulatorChannelHandler(networkEmulatorSettings) : null;
    this.messageHandler = new MessageReceiverChannelHandler(incomingMessagesSubject);
    this.bootstrapFactory = new NettyBootstrapFactory(settings);
    this.outgoingChannels = new Memoizer<>(new OutgoingChannelComputable());
  }

  private LogLevel resolveLogLevel(String logLevel) {
    return (logLevel != null && !logLevel.equals("OFF")) ? LogLevel.valueOf(logLevel) : null;
  }

  public static Transport newInstance(TransportEndpoint localEndpoint, TransportSettings settings) {
    return new Transport(localEndpoint, settings);
  }

  @Override
  public TransportEndpoint localEndpoint() {
    return localEndpoint;
  }

  public EventExecutorGroup getEventExecutor() {
    return bootstrapFactory.getWorkerGroup();
  }

  public void setNetworkSettings(TransportEndpoint destination, int lostPercent, int mean) {
    NetworkEmulatorSettings settings = new NetworkEmulatorSettings(lostPercent, mean);
    networkEmulatorSettings.put(destination, settings);
    LOGGER.debug("Set {} for messages to: {}", settings, destination);
  }

  public void blockMessagesTo(TransportEndpoint destination) {
    networkEmulatorSettings.put(destination, new NetworkEmulatorSettings(100, 0));
    LOGGER.debug("Block messages to: {}", destination);
  }

  public void unblockAll() {
    networkEmulatorSettings.clear();
    LOGGER.debug("Unblock all messages");
  }

  @Override
  public final ListenableFuture<Void> start() {
    incomingMessagesSubject.subscribeOn(Schedulers.from(bootstrapFactory.getWorkerGroup()));

    ServerBootstrap server = bootstrapFactory.serverBootstrap().childHandler(incomingChannelInitializer);
    ChannelFuture bindFuture = server.bind(localEndpoint.port());
    final SettableFuture<Void> result = SettableFuture.create();
    bindFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          serverChannel = (ServerChannel) channelFuture.channel();
          LOGGER.info("Bound transport endpoint: {}", localEndpoint);
          result.set(null);
        } else {
          LOGGER.error("Failed to bind transport endpoint: {}, cause: {}", localEndpoint, channelFuture.cause());
          result.setException(channelFuture.cause());
        }
      }
    });
    return result;
  }

  @Override
  public final void stop() {
    stop(null);
  }

  @Override
  public final void stop(@Nullable SettableFuture<Void> promise) {
    // Complete incoming messages observable
    try {
      incomingMessagesSubject.onCompleted();
    } catch (Exception ignore) {
      // ignore
    }

    // close connected channels
    for (TransportEndpoint endpoint : outgoingChannels.keySet()) {
      ChannelFuture channelFuture = outgoingChannels.getIfExists(endpoint);
      if (channelFuture.isSuccess()) {
        channelFuture.channel().close();
      } else {
        channelFuture.addListener(ChannelFutureListener.CLOSE);
      }
    }
    outgoingChannels.clear();

    // close server channel
    if (serverChannel != null) {
      FutureUtils.compose(serverChannel.close(), promise);
    }

    // TODO: shutdown boss/worker threads
  }

  @Nonnull
  @Override
  public final Observable<Message> listen() {
    return incomingMessagesSubject;
  }

  @Override
  public void disconnect(@CheckForNull TransportEndpoint endpoint, @Nullable SettableFuture<Void> promise) {
    checkArgument(endpoint != null);
    ChannelFuture channelFuture = outgoingChannels.remove(endpoint);
    if (channelFuture != null && channelFuture.isSuccess()) {
      FutureUtils.compose(channelFuture.channel().close(), promise);
    } else {
      if (promise != null) {
        promise.set(null);
      }
    }
  }

  // TODO [AK]: Temporary workaround, should be merged with send by endpoint after endpoint id removed
  public void send(@CheckForNull InetSocketAddress address, @CheckForNull Message message) {
    TransportEndpoint endpoint = TransportEndpoint.create("0", address.getHostName(), address.getPort());
    send(endpoint, message);
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
    message.setSender(localEndpoint);

    final ChannelFuture channelFuture = outgoingChannels.get(endpoint);
    if (channelFuture.isSuccess()) {
      FutureUtils.compose(channelFuture.channel().writeAndFlush(message), promise);
    } else {
      channelFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) {
          if (channelFuture.isSuccess()) {
            FutureUtils.compose(channelFuture.channel().writeAndFlush(message), promise);
          } else {
            promise.setException(channelFuture.cause());
          }
        }
      });
    }
  }

  private final class OutgoingChannelComputable implements Computable<TransportEndpoint, ChannelFuture> {
    @Override
    public ChannelFuture compute(final TransportEndpoint endpoint) throws Exception {
      OutgoingChannelInitializer channelInitializer = new OutgoingChannelInitializer(endpoint);
      Bootstrap client = bootstrapFactory.clientBootstrap().handler(channelInitializer);
      ChannelFuture connectFuture = client.connect(endpoint.host(), endpoint.port());

      // Register logger and cleanup listener
      connectFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) {
          if (channelFuture.isSuccess()) {
            LOGGER.info("Connected transport endpoint: {} {}", endpoint, channelFuture.channel());
          } else {
            LOGGER.warn("Failed to connect transport endpoint: {}", endpoint);
            outgoingChannels.delete(endpoint);
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
      pipeline.addLast("frameDecoder", protocol.getFrameHandlerFactory().newFrameDecoder());
      pipeline.addLast("deserializer", deserializerHandler);
      if (loggingHandler != null) {
        pipeline.addLast("loggingHandler", loggingHandler);
      }
      pipeline.addLast("messageReceiver", messageHandler);
      pipeline.addLast("exceptionHandler", exceptionHandler);
    }
  }

  @ChannelHandler.Sharable
  private final class OutgoingChannelInitializer extends ChannelInitializer {

    private final TransportEndpoint endpoint;

    public OutgoingChannelInitializer(TransportEndpoint endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ChannelDuplexHandler() {
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
          LOGGER.debug("Disconnected transport endpoint: {} {}", endpoint, ctx.channel());
          outgoingChannels.delete(endpoint);
          super.channelInactive(ctx);
        }
      });
      pipeline.addLast(protocol.getFrameHandlerFactory().newFrameEncoder());
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
