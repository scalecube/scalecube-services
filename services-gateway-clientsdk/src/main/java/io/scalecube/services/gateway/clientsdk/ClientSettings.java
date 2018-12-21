package io.scalecube.services.gateway.clientsdk;

import io.scalecube.services.gateway.clientsdk.exceptions.ClientErrorMapper;
import io.scalecube.services.gateway.clientsdk.exceptions.DefaultClientErrorMapper;
import java.net.InetSocketAddress;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;

public class ClientSettings {

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_CONTENT_TYPE = "application/json";

  private final String host;
  private final int port;
  private final String contentType;
  private final LoopResources loopResources;
  private final boolean followRedirect;
  private final SslProvider sslProvider;
  private final ClientErrorMapper errorMapper;

  private ClientSettings(Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.contentType = builder.contentType;
    this.loopResources = builder.loopResources;
    this.followRedirect = builder.followRedirect;
    this.sslProvider = builder.sslProvider;
    this.errorMapper = builder.errorMapper;
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

  public LoopResources loopResources() {
    return loopResources;
  }

  public boolean followRedirect() {
    return followRedirect;
  }

  public SslProvider sslProvider() {
    return sslProvider;
  }

  public ClientErrorMapper errorMapper() {
    return errorMapper;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ClientSettings{");
    sb.append("host='").append(host).append('\'');
    sb.append(", port=").append(port);
    sb.append(", contentType='").append(contentType).append('\'');
    sb.append(", loopResources=").append(loopResources);
    sb.append(", followRedirect=").append(followRedirect);
    sb.append(", sslProvider=").append(sslProvider);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {
    private String host = DEFAULT_HOST;
    private int port;
    private String contentType = DEFAULT_CONTENT_TYPE;
    private LoopResources loopResources;
    private boolean followRedirect = true;
    private SslProvider sslProvider;
    private ClientErrorMapper errorMapper = DefaultClientErrorMapper.INSTANCE;

    private Builder() {}

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder address(InetSocketAddress address) {
      return host(address.getHostString()).port(address.getPort());
    }

    public Builder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = loopResources;
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

    public Builder errorMapper(ClientErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public ClientSettings build() {
      return new ClientSettings(this);
    }
  }
}
