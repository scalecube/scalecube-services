package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.memoizer.Computable;
import io.scalecube.transport.memoizer.Memoizer;

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
import io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Transport implements ITransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private final Address localAddress;
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

  private ServerChannel serverChannel;

  private Transport(Address localAddress, TransportConfig config) {
    checkArgument(localAddress != null);
    checkArgument(config != null);
    this.localAddress = localAddress;
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

  public static Transport newInstance(Address localAddress, TransportConfig config) {
    return new Transport(localAddress, config);
  }

  @Override
  public Address localAddress() {
    return localAddress;
  }

  public EventExecutorGroup getWorkerGroup() {
    return bootstrapFactory.getWorkerGroup();
  }

  /**
   * Sets given network emulator settings. If network emulator is disabled do nothing.
   */
  public void setNetworkSettings(Address destination, int lostPercent, int meanDelay) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.setNetworkSettings(destination, lostPercent, meanDelay);
    } else {
      LOGGER.warn("Network emulator is disabled: can't set network settings");
    }
  }

  /**
   * Sets default network emulator settings. If network emulator is disabled do nothing.
   */
  public void setDefaultNetworkSettings(int lostPercent, int meanDelay) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.setDefaultNetworkSettings(lostPercent, meanDelay);
    } else {
      LOGGER.warn("Network emulator is disabled: can't set default network settings");
    }
  }

  /**
   * Block messages to given destination. If network emulator is disabled do nothing.
   */
  public void blockMessagesTo(Address destination) {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.blockMessagesTo(destination);
    } else {
      LOGGER.warn("Network emulator is disabled: can't block messages");
    }
  }

  /**
   * Unblock messages to all destinations. If network emulator is disabled do nothing.
   */
  public void unblockAll() {
    if (config.isUseNetworkEmulator()) {
      networkEmulatorHandler.unblockAll();
    } else {
      LOGGER.warn("Network emulator is disabled: can't unblock messages");
    }
  }

  @Override
  public final ListenableFuture<Void> start() {
    incomingMessagesSubject.subscribeOn(Schedulers.from(bootstrapFactory.getWorkerGroup()));

    ServerBootstrap server = bootstrapFactory.serverBootstrap().childHandler(incomingChannelInitializer);
    ChannelFuture bindFuture = server.bind(localAddress.port());
    final SettableFuture<Void> result = SettableFuture.create();
    bindFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          serverChannel = (ServerChannel) channelFuture.channel();
          LOGGER.info("Bound to: {}", localAddress);
          result.set(null);
        } else {
          LOGGER.error("Failed to bind to: {}, cause: {}", localAddress, channelFuture.cause());
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
    for (Address address : outgoingChannels.keySet()) {
      ChannelFuture channelFuture = outgoingChannels.getIfExists(address);
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

    // TODO [AK]: shutdown boss/worker threads
  }

  @Nonnull
  @Override
  public final Observable<Message> listen() {
    return incomingMessagesSubject;
  }

  @Override
  public void disconnect(@CheckForNull Address address, @Nullable SettableFuture<Void> promise) {
    checkArgument(address != null);
    ChannelFuture channelFuture = outgoingChannels.remove(address);
    if (channelFuture != null && channelFuture.isSuccess()) {
      composeFutures(channelFuture.channel().close(), promise);
    } else {
      if (promise != null) {
        promise.set(null);
      }
    }
  }

  @Override
  public void send(@CheckForNull Address address, @CheckForNull Message message) {
    send(address, message, null);
  }

  @Override
  public void send(@CheckForNull final Address address, @CheckForNull final Message message,
                   @Nullable final SettableFuture<Void> promise) {
    checkArgument(address != null);
    checkArgument(message != null);
    message.setSender(localAddress);

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
            LOGGER.info("Connected to: {} {}", address, channelFuture.channel());
          } else {
            LOGGER.warn("Failed to connect to: {}", address);
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
