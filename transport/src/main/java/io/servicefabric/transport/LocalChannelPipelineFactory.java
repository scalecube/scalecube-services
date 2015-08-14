package io.servicefabric.transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutorGroup;

public final class LocalChannelPipelineFactory implements PipelineFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(LocalChannelPipelineFactory.class);

	private final Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings = new ConcurrentHashMap<>();

	public LocalChannelPipelineFactory() {
	}

	@Override
	public void setAcceptorPipeline(Channel channel, ITransportSpi transportSpi) {
		ChannelPipeline pipeline = channel.pipeline();
		if (transportSpi.getLogLevel() != null) {
			pipeline.addLast("loggingHandler", new LoggingHandler(transportSpi.getLogLevel()));
		}
		pipeline.addLast("acceptorRegistrator", new AcceptorRegistratorChannelHandler(transportSpi));
		pipeline.addLast(transportSpi.getEventExecutor(), "handshakeHandler", new AcceptorHandshakeChannelHandler(transportSpi));
		pipeline.addLast("exceptionHandler", new ExceptionCaughtChannelHandler(transportSpi));
	}

	@Override
	public void setConnectorPipeline(Channel channel, ITransportSpi transportSpi) {
		ChannelPipeline pipeline = channel.pipeline();
		if (transportSpi.getLogLevel() != null) {
			pipeline.addLast("loggingHandler", new LoggingHandler(transportSpi.getLogLevel()));
		}
		pipeline.addLast(transportSpi.getEventExecutor(), "handshakeHandler", new ConnectorHandshakeChannelHandler(transportSpi));
		pipeline.addLast("exceptionHandler", new ExceptionCaughtChannelHandler(transportSpi));
	}

	@Override
	public void resetDueHandshake(Channel channel, ITransportSpi transportSpi) {
		ChannelPipeline pipeline = channel.pipeline();
		pipeline.addFirst("networkEmulator", new NetworkEmulatorChannelHandler(transportSpi, networkSettings));
		pipeline.remove("handshakeHandler");
		EventExecutorGroup eventExecutor = transportSpi.getEventExecutor();
		pipeline.addBefore(eventExecutor, "exceptionHandler", "messageReceiver", new MessageReceiverChannelHandler(transportSpi));
	}

	public void setNetworkSettings(TransportEndpoint endpoint, int lostPercent, int mean) {
		networkSettings.put(endpoint, new NetworkEmulatorSettings(lostPercent, mean));
	}

	public void blockMessagesTo(TransportEndpoint destination) {
		networkSettings.put(destination, new NetworkEmulatorSettings(100, 0));
		LOGGER.debug("Set BLOCK messages to {}", destination);
	}

	public void unblockMessagesTo(TransportEndpoint destination) {
		networkSettings.put(destination, new NetworkEmulatorSettings(0, 0));
		LOGGER.debug("Set UNBLOCK messages to {}", destination);
	}

	public void unblockAll() {
		networkSettings.clear();
		LOGGER.debug("Set UNBLOCK ALL messages");
	}
}
