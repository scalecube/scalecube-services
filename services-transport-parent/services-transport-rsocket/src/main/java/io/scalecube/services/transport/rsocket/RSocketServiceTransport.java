package io.scalecube.services.transport.rsocket;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;
import reactor.netty.channel.AbortedException;
import reactor.netty.resources.LoopResources;

public class RSocketServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceTransport.class);

  static {
    Hooks.onErrorDropped(
        t -> {
          if (AbortedException.isConnectionReset(t)
              || ConnectionClosedException.isConnectionClosed(t)) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Connection aborted: {}", t.toString());
            }
          }
        });
  }

  public static final int DEFAULT_MTU = 0;
  public static final int DEFAULT_MAX_MESSAGE_SIZE = 0;

  public static final String NUM_OF_WORKERS_PROP_NAME = "scalecube.services.transport.numOfWorkers";
  public static final String ALLOWED_ROLES_PROP_NAME = "scalecube.services.transport.allowedRoles";
  public static final String MTU_PROP_NAME = "scalecube.services.transport.mtu";
  public static final String MAX_MESSAGE_SIZE_PROP_NAME =
      "scalecube.services.transport.maxMessageSize";

  private int numOfWorkers;
  private HeadersCodec headersCodec = HeadersCodec.DEFAULT_INSTANCE;
  private Collection<DataCodec> dataCodecs = DataCodec.getAllInstances();
  private CredentialsSupplier credentialsSupplier;
  private Authenticator authenticator;
  private Collection<String> allowedRoles;
  private int mtu;
  private int maxMessageSize;

  private Function<LoopResources, RSocketServerTransportFactory> serverTransportFactory =
      RSocketServerTransportFactory.websocket();
  private Function<LoopResources, RSocketClientTransportFactory> clientTransportFactory =
      RSocketClientTransportFactory.websocket();

  // resources
  private EventLoopGroup eventLoopGroup;
  private LoopResources clientLoopResources;
  private LoopResources serverLoopResources;

  public RSocketServiceTransport() {
    this(System.getProperties());
  }

  public RSocketServiceTransport(Properties properties) {
    numOfWorkers(properties);
    allowedRoles(properties);
    mtu(properties);
    maxMessageSize(properties);
  }

  private static String getProperty(Properties properties, String name) {
    final var value = properties.getProperty(name);
    return "@null".equals(value) ? null : value;
  }

  private static int getProperty(Properties properties, String name, int defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  public int numOfWorkers() {
    return numOfWorkers;
  }

  public RSocketServiceTransport numOfWorkers(Properties properties) {
    return numOfWorkers(
        getProperty(
            properties, NUM_OF_WORKERS_PROP_NAME, Runtime.getRuntime().availableProcessors()));
  }

  public Collection<String> allowedRoles() {
    return allowedRoles;
  }

  /**
   * Reads comma-separated allowed roles. Absent (or {@code @null}) leaves roles unrestricted.
   *
   * @param properties properties
   * @return this
   */
  public RSocketServiceTransport allowedRoles(Properties properties) {
    final var value = getProperty(properties, ALLOWED_ROLES_PROP_NAME);
    if (value == null) {
      this.allowedRoles = null;
      return this;
    }
    return allowedRoles(
        Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList());
  }

  public int mtu() {
    return mtu;
  }

  public RSocketServiceTransport mtu(Properties properties) {
    return mtu(getProperty(properties, MTU_PROP_NAME, DEFAULT_MTU));
  }

  public int maxMessageSize() {
    return maxMessageSize;
  }

  public RSocketServiceTransport maxMessageSize(Properties properties) {
    return maxMessageSize(
        getProperty(properties, MAX_MESSAGE_SIZE_PROP_NAME, DEFAULT_MAX_MESSAGE_SIZE));
  }

  /**
   * Setter for {@code numOfWorkers}.
   *
   * @param numOfWorkers number of worker threads
   * @return this
   */
  public RSocketServiceTransport numOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
    return this;
  }

  /**
   * Setter for {@code headersCodec}.
   *
   * @param headersCodec headers codec
   * @return this
   */
  public RSocketServiceTransport headersCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
    return this;
  }

  /**
   * Setter for {@code dataCodecs}.
   *
   * @param dataCodecs set of data codecs
   * @return this
   */
  public RSocketServiceTransport dataCodecs(Collection<DataCodec> dataCodecs) {
    this.dataCodecs = dataCodecs;
    return this;
  }

  /**
   * Setter for {@code credentialsSupplier}.
   *
   * @param credentialsSupplier credentialsSupplier
   * @return this
   */
  public RSocketServiceTransport credentialsSupplier(CredentialsSupplier credentialsSupplier) {
    this.credentialsSupplier = credentialsSupplier;
    return this;
  }

  /**
   * Setter for {@code authenticator}.
   *
   * @param authenticator authenticator
   * @return this
   */
  public RSocketServiceTransport authenticator(Authenticator authenticator) {
    this.authenticator = authenticator;
    return this;
  }

  /**
   * Setter for {@code serverTransportFactory}.
   *
   * @param serverTransportFactory serverTransportFactory
   * @return this
   */
  public RSocketServiceTransport serverTransportFactory(
      Function<LoopResources, RSocketServerTransportFactory> serverTransportFactory) {
    this.serverTransportFactory = serverTransportFactory;
    return this;
  }

  /**
   * Setter for {@code clientTransportFactory}.
   *
   * @param clientTransportFactory clientTransportFactory
   * @return this
   */
  public RSocketServiceTransport clientTransportFactory(
      Function<LoopResources, RSocketClientTransportFactory> clientTransportFactory) {
    this.clientTransportFactory = clientTransportFactory;
    return this;
  }

  /**
   * Setter for {@code allowedRoles}.
   *
   * @param allowedRoles allowedRoles
   * @return this
   */
  public RSocketServiceTransport allowedRoles(Collection<String> allowedRoles) {
    this.allowedRoles = new HashSet<>(allowedRoles);
    return this;
  }

  /**
   * Setter for {@code mtu} (fragmentation MTU, in bytes). RSocket frames larger than this are
   * fragmented on send and reassembled on receive; {@code 0} (the default) disables fragmentation.
   * Applies to both the client and server transports. A non-zero value must be in {@code [64,
   * 2^24-1)} (RSocket's minimum fragment size, and below the single-frame cap); other values are
   * rejected up front rather than failing deep inside RSocket at bind/connect.
   *
   * @param mtu fragmentation MTU in bytes ({@code 0} disables fragmentation, otherwise {@code [64,
   *     2^24-1)})
   * @return this
   * @throws IllegalArgumentException if {@code mtu} is non-zero and outside {@code [64, 2^24-1)}
   */
  public RSocketServiceTransport mtu(int mtu) {
    if (mtu != 0 && (mtu < RSocketConstants.MIN_MTU || mtu >= RSocketConstants.MAX_FRAME_LENGTH)) {
      throw new IllegalArgumentException(
          "mtu must be 0 or in ["
              + RSocketConstants.MIN_MTU
              + ", "
              + RSocketConstants.MAX_FRAME_LENGTH
              + "): "
              + mtu);
    }
    this.mtu = mtu;
    return this;
  }

  /**
   * Setter for {@code maxMessageSize} (the high-watermark, in bytes). When {@code > 0}, this bounds
   * the encoded <em>data</em> payload of a single message (the message's headers/metadata are not
   * counted toward the limit, so the full on-wire payload is slightly larger): an outbound message
   * whose encoded data exceeds this fails fast while encoding with a {@code 413} service error
   * (never framed, fragmented, or fully buffered), preventing OOM on the sender. When it is also
   * {@code >=} the single-frame cap ({@code 2^24 - 1}), inbound reassembly is additionally capped
   * at the same size (see {@link io.rsocket.core.RSocketServer#maxInboundPayloadSize(int)}),
   * preventing OOM on the receiver; RSocket forbids an inbound cap below the frame size (a single
   * frame must always fit), so a watermark below the frame cap bounds only the outbound encode (so
   * to cap what you <em>receive</em>, the value must be {@code >=} the frame cap). {@code 0} (the
   * default) means unbounded. Set it above the single-frame cap and pair it with {@link #mtu(int)}
   * to allow legitimately large (fragmented) responses up to the watermark while still rejecting
   * anything beyond it.
   *
   * <p>Two caveats. (1) Because the limit counts only the encoded data, a value near the frame cap
   * <em>without</em> {@link #mtu(int)} can still overflow the real single-frame limit (data +
   * headers + framing) and surface the cryptic RSocket {@code CanceledException} this is meant to
   * replace; to use it as a {@code CanceledException} replacement without fragmentation, keep it
   * safely below the frame cap. (2) The {@code 413} does not surface as an error on {@code
   * byte[]}-typed service methods (the client decodes a {@code byte[]} response before checking the
   * error flag) until that separate fix lands.
   *
   * @param maxMessageSize maximum message size in bytes ({@code 0} means unbounded)
   * @return this
   * @throws IllegalArgumentException if {@code maxMessageSize} is negative
   */
  public RSocketServiceTransport maxMessageSize(int maxMessageSize) {
    if (maxMessageSize < 0) {
      throw new IllegalArgumentException("maxMessageSize must be >= 0: " + maxMessageSize);
    }
    this.maxMessageSize = maxMessageSize;
    return this;
  }

  @Override
  public ClientTransport clientTransport() {
    return new RSocketClientTransport(
        headersCodec,
        dataCodecs,
        clientTransportFactory.apply(clientLoopResources),
        credentialsSupplier,
        allowedRoles,
        mtu,
        maxMessageSize);
  }

  @Override
  public ServerTransport serverTransport(ServiceRegistry serviceRegistry) {
    return new RSocketServerTransport(
        authenticator,
        serviceRegistry,
        headersCodec,
        dataCodecs,
        serverTransportFactory.apply(serverLoopResources),
        mtu,
        maxMessageSize);
  }

  @Override
  public ServiceTransport start() {
    eventLoopGroup = newEventLoopGroup();
    clientLoopResources = DelegatedLoopResources.newClientLoopResources(eventLoopGroup);
    serverLoopResources = DelegatedLoopResources.newServerLoopResources(eventLoopGroup);
    return this;
  }

  @Override
  public void stop() {
    if (serverLoopResources != null && eventLoopGroup != null) {
      if (!serverLoopResources.isDisposed()) {
        serverLoopResources.dispose();
      }
      if (!eventLoopGroup.isShutdown()) {
        eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
      }
    }
  }

  private EventLoopGroup newEventLoopGroup() {
    ThreadFactory threadFactory = new DefaultThreadFactory("rsocket-worker", true);
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup(numOfWorkers, threadFactory);
    return LoopResources.colocate(eventLoopGroup);
  }
}
