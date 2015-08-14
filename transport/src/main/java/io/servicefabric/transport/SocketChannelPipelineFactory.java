package io.servicefabric.transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.servicefabric.transport.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutorGroup;

public final class SocketChannelPipelineFactory implements PipelineFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(SocketChannelPipelineFactory.class);

	private final Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings = new ConcurrentHashMap<>();
	private FrameHandlerFactory frameHandlerFactory;
	private MessageToByteEncoder<Message> serializerHandler;
	private MessageToMessageDecoder<ByteBuf> deserializerHandler;
	private boolean useNetworkEmulator;

	private SocketChannelPipelineFactory() {
	}

	public static Builder builder() {
		return new Builder();
	}

	public void setSerializer(MessageSerializer serializer) {
		this.serializerHandler = new SharableSerializerHandler(serializer);
	}

	public void setDeserializer(MessageDeserializer deserializer) {
		this.deserializerHandler = new SharableDeserializerHandler(deserializer);
	}

	public void setFrameHandlerFactory(FrameHandlerFactory frameHandlerFactory) {
		this.frameHandlerFactory = frameHandlerFactory;
	}

	@Override
	public void setAcceptorPipeline(Channel channel, ITransportSpi transportSpi) {
		ChannelPipeline pipeline = channel.pipeline();
		prepareForGenericDataFormat(pipeline);
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
		prepareForGenericDataFormat(pipeline);
		if (transportSpi.getLogLevel() != null) {
			pipeline.addLast("loggingHandler", new LoggingHandler(transportSpi.getLogLevel()));
		}
		pipeline.addLast(transportSpi.getEventExecutor(), "handshakeHandler", new ConnectorHandshakeChannelHandler(transportSpi));
		pipeline.addLast("exceptionHandler", new ExceptionCaughtChannelHandler(transportSpi));
	}

	@Override
	public void resetDueHandshake(Channel channel, ITransportSpi transportSpi) {
		ChannelPipeline pipeline = channel.pipeline();
		EventExecutorGroup eventExecutor = transportSpi.getEventExecutor();
		if (useNetworkEmulator) {
			NetworkEmulatorChannelHandler networkEmulator = new NetworkEmulatorChannelHandler(transportSpi, networkSettings);
			pipeline.addBefore("handshakeHandler", "networkEmulator", networkEmulator);
		}
		pipeline.remove("handshakeHandler");
		pipeline.addBefore(eventExecutor, "exceptionHandler", "messageReceiver", new MessageReceiverChannelHandler(transportSpi));
	}

	private void prepareForGenericDataFormat(ChannelPipeline pipeline) {
		pipeline.addLast("frameDecoder", frameHandlerFactory.getFrameDecoder());
		pipeline.addLast("deserializer", deserializerHandler);
		pipeline.addLast("frameEncoder", frameHandlerFactory.getFrameEncoder());
		pipeline.addLast("serializer", serializerHandler);
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

	public static final class Builder {
		private final SocketChannelPipelineFactory target = new SocketChannelPipelineFactory();

		private Builder() {
		}

		public Builder set(FrameHandlerFactory frameHandlerFactory) {
			target.setFrameHandlerFactory(frameHandlerFactory);
			return this;
		}

		public Builder set(MessageDeserializer deserializer) {
			target.setDeserializer(deserializer);
			return this;
		}

		public Builder set(MessageSerializer serializer) {
			target.setSerializer(serializer);
			return this;
		}

		public Builder useNetworkEmulator() {
			target.useNetworkEmulator = true;
			return this;
		}

		public SocketChannelPipelineFactory build() {
			return target;
		}
	}
}
