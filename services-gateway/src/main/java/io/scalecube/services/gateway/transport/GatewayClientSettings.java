package io.scalecube.services.gateway.transport;

import io.scalecube.net.Address;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import reactor.netty.tcp.SslProvider;

public class GatewayClientSettings {

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_CONTENT_TYPE = "application/json";
  private static final Duration DEFAULT_KEEPALIVE_INTERVAL = Duration.ZERO;

  private final String host;
  private final int port;
  private final String contentType;
  private final boolean followRedirect;
  private final SslProvider sslProvider;
  private final ServiceClientErrorMapper errorMapper;
  private final Duration keepAliveInterval;
  private final boolean wiretap;
  private final Map<String, String> headers;

  private GatewayClientSettings(Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.contentType = builder.contentType;
    this.followRedirect = builder.followRedirect;
    this.sslProvider = builder.sslProvider;
    this.errorMapper = builder.errorMapper;
    this.keepAliveInterval = builder.keepAliveInterval;
    this.wiretap = builder.wiretap;
    this.headers = builder.headers;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public String contentType() {
    return this.contentType;
  }

  public boolean followRedirect() {
    return followRedirect;
  }

  public SslProvider sslProvider() {
    return sslProvider;
  }

  public ServiceClientErrorMapper errorMapper() {
    return errorMapper;
  }

  public Duration keepAliveInterval() {
    return this.keepAliveInterval;
  }

  public boolean wiretap() {
    return this.wiretap;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder from(GatewayClientSettings gatewayClientSettings) {
    return new Builder(gatewayClientSettings);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GatewayClientSettings{");
    sb.append("host='").append(host).append('\'');
    sb.append(", port=").append(port);
    sb.append(", contentType='").append(contentType).append('\'');
    sb.append(", followRedirect=").append(followRedirect);
    sb.append(", keepAliveInterval=").append(keepAliveInterval);
    sb.append(", wiretap=").append(wiretap);
    sb.append(", sslProvider=").append(sslProvider);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private String host = DEFAULT_HOST;
    private int port;
    private String contentType = DEFAULT_CONTENT_TYPE;
    private boolean followRedirect = true;
    private SslProvider sslProvider;
    private ServiceClientErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private Duration keepAliveInterval = DEFAULT_KEEPALIVE_INTERVAL;
    private boolean wiretap = false;
    private Map<String, String> headers = Collections.emptyMap();

    private Builder() {}

    private Builder(GatewayClientSettings originalSettings) {
      this.host = originalSettings.host;
      this.port = originalSettings.port;
      this.contentType = originalSettings.contentType;
      this.followRedirect = originalSettings.followRedirect;
      this.sslProvider = originalSettings.sslProvider;
      this.errorMapper = originalSettings.errorMapper;
      this.keepAliveInterval = originalSettings.keepAliveInterval;
      this.wiretap = originalSettings.wiretap;
      this.headers = Collections.unmodifiableMap(new HashMap<>(originalSettings.headers));
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder address(Address address) {
      return host(address.host()).port(address.port());
    }

    public Builder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    /**
     * Specifies is auto-redirect enabled for HTTP 301/302 status codes. Enabled by default.
     *
     * @param followRedirect if <code>true</code> auto-redirect is enabled, otherwise disabled
     * @return builder
     */
    public Builder followRedirect(boolean followRedirect) {
      this.followRedirect = followRedirect;
      return this;
    }

    /**
     * Use default SSL client provider.
     *
     * @return builder
     */
    public Builder secure() {
      this.sslProvider = SslProvider.defaultClientProvider();
      return this;
    }

    /**
     * Use specified SSL provider.
     *
     * @param sslProvider SSL provider
     * @return builder
     */
    public Builder secure(SslProvider sslProvider) {
      this.sslProvider = sslProvider;
      return this;
    }

    /**
     * Keepalive interval. If client's channel doesn't have any activity at channel during this
     * period, it will send a keepalive message to the server.
     *
     * @param keepAliveInterval keepalive interval.
     * @return builder
     */
    public Builder keepAliveInterval(Duration keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    /**
     * Specifies whether to enaple 'wiretap' option for connections. That logs full netty traffic.
     * Default is {@code false}
     *
     * @param wiretap whether to enable 'wiretap' handler at connection. Default - false
     * @return builder
     */
    public Builder wiretap(boolean wiretap) {
      this.wiretap = wiretap;
      return this;
    }

    public Builder errorMapper(ServiceClientErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
      return this;
    }

    public GatewayClientSettings build() {
      return new GatewayClientSettings(this);
    }
  }
}
