package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.servicefabric.transport.protocol.ProtostuffFrameHandlerFactory;
import io.servicefabric.transport.protocol.ProtostuffMessageDeserializer;
import io.servicefabric.transport.protocol.ProtostuffMessageSerializer;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Anton Kharenko
 */
public class TransportBuilder {

  private final TransportEndpoint localEndpoint;
  private final String localEndpointIncarnationId;
  private final EventLoopGroup eventLoop;
  private final EventExecutorGroup eventExecutor;

  private TransportSettings transportSettings;
  private boolean useNetworkEmulator = false;

  private TransportBuilder(TransportEndpoint localEndpoint, String localEndpointIncarnationId) {
    checkArgument(localEndpoint != null);
    checkArgument(localEndpointIncarnationId != null);
    this.localEndpoint = localEndpoint;
    this.localEndpointIncarnationId = localEndpointIncarnationId;
    this.eventLoop = null;
    this.eventExecutor = null;
  }

  private TransportBuilder(TransportEndpoint localEndpoint, String localEndpointIncarnationId, EventLoopGroup eventLoop,
      EventExecutorGroup eventExecutor) {
    checkArgument(localEndpoint != null);
    checkArgument(localEndpointIncarnationId != null);
    checkArgument(eventLoop != null);
    checkArgument(eventExecutor != null);
    this.localEndpoint = localEndpoint;
    this.localEndpointIncarnationId = localEndpointIncarnationId;
    this.eventLoop = eventLoop;
    this.eventExecutor = eventExecutor;
  }

  public static TransportBuilder newInstance(TransportEndpoint localEndpoint, String endpointId) {
    return new TransportBuilder(localEndpoint, endpointId);
  }

  public static TransportBuilder newInstance(TransportEndpoint localEndpoint, String endpointId, EventLoopGroup eventLoop,
      EventExecutorGroup eventExecutor) {
    return new TransportBuilder(localEndpoint, endpointId, eventLoop, eventExecutor);
  }

  public void setTransportSettings(TransportSettings transportSettings) {
    this.transportSettings = transportSettings;
  }

  public void setUseNetworkEmulator(boolean useNetworkEmulator) {
    this.useNetworkEmulator = useNetworkEmulator;
  }

  public TransportBuilder transportSettings(TransportSettings transportSettings) {
    this.transportSettings = transportSettings;
    return this;
  }

  public TransportBuilder useNetworkEmulator() {
    setUseNetworkEmulator(true);
    return this;
  }

  public ITransport build() {
    // Create transport
    Transport transport = (eventLoop == null) ? new Transport(localEndpoint) : new Transport(localEndpoint, eventLoop, eventExecutor);

    // Set parameters
    if (transportSettings != null) {
      transport.setConnectTimeout(transportSettings.getConnectTimeout());
      transport.setHandshakeTimeout(transportSettings.getHandshakeTimeout());
      transport.setSendHwm(transportSettings.getSendHighWaterMark());
      transport.setLogLevel(transportSettings.getLogLevel());
    } else {
      transport.setConnectTimeout(TransportSettings.DEFAULT_CONNECT_TIMEOUT);
      transport.setHandshakeTimeout(TransportSettings.DEFAULT_HANDSHAKE_TIMEOUT);
      transport.setSendHwm(TransportSettings.DEFAULT_SEND_HIGH_WATER_MARK);
      transport.setLogLevel(TransportSettings.DEFAULT_LOG_LEVEL);
    }

    // Set pipeline
    SocketChannelPipelineFactory.Builder pipelineFactoryBuilder =
        SocketChannelPipelineFactory.builder().set(new ProtostuffFrameHandlerFactory()).set(
            new ProtostuffMessageSerializer())
            .set(new ProtostuffMessageDeserializer());

    if (useNetworkEmulator) {
      pipelineFactoryBuilder.useNetworkEmulator();
    }

    transport.setPipelineFactory(pipelineFactoryBuilder.build());

    // Set metadata
    Map<String, Object> metadata = new HashMap<>();
    metadata.put(TransportData.META_ORIGIN_ENDPOINT, localEndpoint);
    metadata.put(TransportData.META_ORIGIN_ENDPOINT_ID, localEndpointIncarnationId);
    transport.setLocalMetadata(metadata);

    return transport;
  }

  public static class TransportSettings {

    public static final int DEFAULT_CONNECT_TIMEOUT = 3000;
    public static final int DEFAULT_HANDSHAKE_TIMEOUT = 1000;
    public static final int DEFAULT_SEND_HIGH_WATER_MARK = 1000;
    public static final String DEFAULT_LOG_LEVEL = "OFF";

    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT;
    private int sendHighWaterMark = DEFAULT_SEND_HIGH_WATER_MARK;
    private String logLevel = DEFAULT_LOG_LEVEL;

    public TransportSettings() {}

    public TransportSettings(int connectTimeout, int handshakeTimeout, int sendHighWaterMark, String logLevel) {
      this.connectTimeout = connectTimeout;
      this.handshakeTimeout = handshakeTimeout;
      this.sendHighWaterMark = sendHighWaterMark;
      this.logLevel = logLevel;
    }

    public int getConnectTimeout() {
      return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
    }

    public int getHandshakeTimeout() {
      return handshakeTimeout;
    }

    public void setHandshakeTimeout(int handshakeTimeout) {
      this.handshakeTimeout = handshakeTimeout;
    }

    public int getSendHighWaterMark() {
      return sendHighWaterMark;
    }

    public void setSendHighWaterMark(int sendHighWaterMark) {
      this.sendHighWaterMark = sendHighWaterMark;
    }

    public String getLogLevel() {
      return logLevel;
    }

    public void setLogLevel(String logLevel) {
      this.logLevel = logLevel;
    }

    @Override
    public String toString() {
      return "TransportSettings{" + "connectTimeout=" + connectTimeout + ", handshakeTimeout=" + handshakeTimeout + ", sendHighWaterMark="
          + sendHighWaterMark + ", logLevel='" + logLevel + '\'' + '}';
    }
  }
}
