package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static io.servicefabric.transport.TransportChannel.ATTR_TRANSPORT;
import static io.servicefabric.transport.TransportChannel.Builder.ACCEPTOR;
import static io.servicefabric.transport.TransportChannel.Builder.CONNECTOR;
import static io.servicefabric.transport.TransportChannel.Status.CONNECT_FAILED;
import static io.servicefabric.transport.TransportChannel.Status.CONNECT_IN_PROGRESS;
import static io.servicefabric.transport.TransportChannel.Status.NEW;
import static io.servicefabric.transport.TransportData.Q_TRANSPORT_HANDSHAKE_SYNC;
import static io.servicefabric.transport.TransportData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK;
import static io.servicefabric.transport.utils.ChannelFutureUtils.setPromise;
import static io.servicefabric.transport.utils.ChannelFutureUtils.wrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.servicefabric.transport.utils.memoization.Computable;
import io.servicefabric.transport.utils.memoization.ConcurrentMapMemoizer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class Transport implements ITransportSpi, ITransport {
	private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

	private final TransportEndpoint localEndpoint;
	private final EventLoopGroup eventLoop;
	private final EventExecutorGroup eventExecutor;
	private Class<? extends Channel> clientChannelClass;
	private ServerChannel serverChannel;
	private int connectTimeout = 3000;
	private int handshakeTimeout = 1000;
	private int sendHwm = 1000;
	private LogLevel logLevel;
	private Map<String, Object> localMetadata = new HashMap<>();
	private PipelineFactory pipelineFactory;
	private final Subject<TransportMessage, TransportMessage> subject = PublishSubject.create();
	private final ConcurrentMap<TransportEndpoint, TransportChannel> accepted = new ConcurrentHashMap<>();
	private final ConcurrentMapMemoizer<TransportEndpoint, TransportChannel> connected = new ConcurrentMapMemoizer<>();

	Transport(TransportEndpoint localEndpoint) {
		checkArgument(localEndpoint != null);
		this.localEndpoint = localEndpoint;
		ThreadFactory eventLoopThreadFactory = createThreadFactory("servicefabric-transport-io-%s@" + localEndpoint);
		switch (localEndpoint.getScheme()) {
		case "local":
			eventLoop = new LocalEventLoopGroup(1, eventLoopThreadFactory);
			break;
		case "tcp":
			eventLoop = new NioEventLoopGroup(1, eventLoopThreadFactory);
			break;
		default:
			throw new IllegalArgumentException(localEndpoint.toString());
		}
		ThreadFactory eventExecutorThreadFactory = createThreadFactory("servicefabric-transport-exec-%s@" + localEndpoint);
		eventExecutor = new DefaultEventExecutorGroup(1, eventExecutorThreadFactory);
	}

	Transport(TransportEndpoint localEndpoint, EventLoopGroup eventLoop, EventExecutorGroup eventExecutor) {
		checkArgument(localEndpoint != null);
		checkArgument(eventLoop != null);
		checkArgument(eventExecutor != null);
		this.localEndpoint = localEndpoint;
		this.eventLoop = eventLoop;
		this.eventExecutor = eventExecutor;
	}

	public static ThreadFactory createThreadFactory(String namingFormat) {
		return new ThreadFactoryBuilder()
				.setNameFormat(namingFormat)
				.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
					@Override
					public void uncaughtException(Thread t, Throwable e) {
						LOGGER.error("Unhandled exception: {}", e, e);
					}
				}).setDaemon(true).build();
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public void setHandshakeTimeout(int handshakeTimeout) {
		this.handshakeTimeout = handshakeTimeout;
	}

	public void setSendHwm(int sendHwm) {
		this.sendHwm = sendHwm;
	}

	public void setLogLevel(String logLevel) {
		if (logLevel != null && !logLevel.equals("OFF")) {
			this.logLevel = LogLevel.valueOf(logLevel);
		}
	}

	public void setLocalMetadata(Map<String, Object> localMetadata) {
		this.localMetadata = new HashMap<>(localMetadata);
	}

	public void setPipelineFactory(PipelineFactory pipelineFactory) {
		this.pipelineFactory = pipelineFactory;
	}

	@SuppressWarnings("unchecked")
	public <T extends PipelineFactory> T getPipelineFactory() {
		return (T) pipelineFactory;
	}

	@Override
	public final void start() {
		checkNotNull(pipelineFactory); // don't forget to set upfront

		// register data types
		TransportTypeRegistry.getInstance().registerType(Q_TRANSPORT_HANDSHAKE_SYNC, TransportData.class);
		TransportTypeRegistry.getInstance().registerType(Q_TRANSPORT_HANDSHAKE_SYNC_ACK, TransportData.class);

		subject.subscribeOn(Schedulers.from(eventExecutor)); // define that we making smart subscribe

		Class<? extends ServerChannel> serverChannelClass;
		SocketAddress bindAddress;
		switch (localEndpoint.getScheme()) {
		case "local":
			clientChannelClass = LocalChannel.class;
			serverChannelClass = LocalServerChannel.class;
			bindAddress = new LocalAddress(localEndpoint.getHostAddress());
			break;
		case "tcp":
			clientChannelClass = NioSocketChannel.class;
			serverChannelClass = NioServerSocketChannel.class;
			bindAddress = new InetSocketAddress(localEndpoint.getPort());
			break;
		default:
			throw new IllegalArgumentException(localEndpoint.toString());
		}

		ServerBootstrap server = new ServerBootstrap();
		server.group(eventLoop).channel(serverChannelClass).childHandler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel channel) {
				pipelineFactory.setAcceptorPipeline(channel, Transport.this);
			}
		});
		try {
			serverChannel = (ServerChannel) server.bind(bindAddress).syncUninterruptibly().channel();
			LOGGER.info("Netty TransportFactory - bound to: {}", bindAddress);
		} catch (Exception e) {
			LOGGER.error("Failed to bind to: " + bindAddress + ", caught " + e, e);
			propagate(e);
		}
	}

	public EventLoopGroup getEventLoop() {
		return eventLoop;
	}

	@Override
	public EventExecutorGroup getEventExecutor() {
		return eventExecutor;
	}

	public final void destroy() {
		stop(null);
	}

	@Nonnull
	@Override
	public final ITransportChannel to(@CheckForNull final TransportEndpoint endpoint) {
		checkArgument(endpoint != null);
		return connected.get(endpoint, new Computable<TransportEndpoint, TransportChannel>() {
			@Override
			public TransportChannel compute(TransportEndpoint arg) {
				final Channel channel = createConnectorChannel();
				final TransportChannel transport = createConnector(channel, endpoint);

				channel.attr(ATTR_TRANSPORT).set(transport);
				LOGGER.debug("Registered connector: {}", transport);

				final ChannelFuture regFuture = eventLoop.register(channel);
				if (regFuture.cause() != null) {
					if (channel.isRegistered()) {
						channel.close();
					} else {
						channel.unsafe().closeForcibly();
					}
					throw new TransportException(transport, regFuture.cause());
				}

				final SocketAddress connectAddress;
				switch (endpoint.getScheme()) {
				case "local":
					connectAddress = new LocalAddress(endpoint.getHostAddress());
					break;
				case "tcp":
					connectAddress = new InetSocketAddress(endpoint.getHostAddress(), endpoint.getPort());
					break;
				default:
					throw new IllegalArgumentException(endpoint.toString());
				}

				final ChannelPromise promise = channel.newPromise();
				if (regFuture.isDone()) {
					connect(regFuture, channel, connectAddress, promise, transport);
				} else {
					regFuture.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							connect(regFuture, channel, connectAddress, promise, transport);
						}
					});
				}
				return transport;
			}
		});
	}

	@Nonnull
	@Override
	public final Observable<TransportMessage> listen() {
		return subject;
	}


	@Override
	public final void stop() {
		stop(null);
	}

	@Override
	public final void stop(@Nullable SettableFuture<Void> promise) {
		try {
			subject.onCompleted();
		} catch (Exception ignore) {
		}
		// cleanup accepted
		for (TransportEndpoint endpoint : accepted.keySet()) {
			TransportChannel transport = accepted.remove(endpoint);
			if (transport != null) {
				transport.close(null, null);
			}
		}
		// cleanup connected
		for (TransportEndpoint endpoint : connected.keySet()) {
			TransportChannel transport = connected.remove(endpoint);
			if (transport != null) {
				transport.close(null, null);
			}
		}
		if (serverChannel != null) {
			setPromise(serverChannel.close(), promise);
		}
	}

	private TransportChannel createConnector(Channel channel, final TransportEndpoint endpoint) {
		TransportChannel.Builder builder = CONNECTOR(channel, this);
		builder.set(new Func1<TransportChannel, Void>() {
			@Override
			public Void call(TransportChannel transport) {
				connected.remove(endpoint);
				return null;
			}
		});
		return builder.build();
	}

	@Override
	public TransportChannel createAcceptor(Channel channel) {
		TransportChannel.Builder builder = ACCEPTOR(channel, this);
		builder.set(new Func1<TransportChannel, Void>() {
			@Override
			public Void call(TransportChannel transport) {
				if (transport.getRemoteEndpoint() != null) {
					accepted.remove(transport.getRemoteEndpoint());
				}
				return null;
			}
		});
		return builder.build();
	}

	@Override
	public void accept(TransportChannel transport) throws TransportBrokenException {
		TransportChannel prev = accepted.putIfAbsent(transport.getRemoteEndpoint(), transport);
		if (prev != null) {
			String err = String.format("Detected duplicate %s for key=%s in accepted_map", prev, transport.getRemoteEndpoint());
			throw new TransportBrokenException(transport, err);
		}
	}

	@Override
	public void resetDueHandshake(Channel channel) {
		pipelineFactory.resetDueHandshake(channel, this);
	}

	@Override
	public final Subject<TransportMessage, TransportMessage> getSubject() {
		return subject;
	}

	@Override
	public final TransportEndpoint getLocalEndpoint() {
		return localEndpoint;
	}

	@Override
	public final Map<String, Object> getLocalMetadata() {
		return new HashMap<>(localMetadata);
	}

	@Override
	public final int getHandshakeTimeout() {
		return handshakeTimeout;
	}

	@Override
	public int getSendHwm() {
		return sendHwm;
	}

	@Override
	public LogLevel getLogLevel() {
		return logLevel;
	}

	@Override
	public final TransportChannel getTransportChannel(Channel channel) {
		TransportChannel transport = channel.attr(ATTR_TRANSPORT).get();
		if (transport == null)
			throw new TransportBrokenException("transport attr not set");
		return transport;
	}

	private Channel createConnectorChannel() {
		Channel channel;
		try {
			channel = clientChannelClass.newInstance();
		} catch (Throwable t) {
			throw new ChannelException("Unable to create Channel from class " + clientChannelClass, t);
		}
		pipelineFactory.setConnectorPipeline(channel, this);
		channel.config().setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
		channel.config().setOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		channel.config().setOption(ChannelOption.TCP_NODELAY, true);
		channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
		channel.config().setOption(ChannelOption.SO_REUSEADDR, true);
		return channel;
	}

	private void connect(final ChannelFuture regFuture,
			final Channel channel,
			final SocketAddress remoteAddress,
			final ChannelPromise promise,
			final TransportChannel transport) {
		eventLoop.execute(new Runnable() {
			@Override
			public void run() {
				if (regFuture.isSuccess()) {
					transport.flip(NEW, CONNECT_IN_PROGRESS);
					channel.connect(remoteAddress, promise);
					promise.addListener(wrap(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							if (!future.isSuccess()) {
								transport.flip(CONNECT_IN_PROGRESS, CONNECT_FAILED);
								transport.close(future.cause());
							}
						}
					}));
				} else {
					channel.unsafe().closeForcibly();
				}
			}
		});
	}
}
