package io.scalecube.services.gateway.http;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.PATCH;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpMethod.TRACE;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerFormDecoderProvider;
import reactor.netty.resources.LoopResources;

public class HttpGateway implements Gateway {

  public static final List<HttpMethod> SUPPORTED_METHODS =
      List.of(GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS, TRACE);

  private final String id;
  private final int port;
  private final Function<ServiceCall, ServiceCall> callFactory;
  private final ServiceProviderErrorMapper errorMapper;
  private final HttpGatewayAuthenticator authenticator;
  private final boolean corsEnabled;
  private final CorsConfigBuilder corsConfigBuilder;
  private final Consumer<HttpServerFormDecoderProvider.Builder> formDecoderBuilder;

  private DisposableServer server;
  private LoopResources loopResources;

  private HttpGateway(Builder builder) {
    this.id = builder.id;
    this.port = builder.port;
    this.callFactory = builder.callFactory;
    this.errorMapper = builder.errorMapper;
    this.authenticator = builder.authenticator;
    this.corsEnabled = builder.corsEnabled;
    this.corsConfigBuilder = builder.corsConfigBuilder;
    this.formDecoderBuilder = builder.formDecoderBuilder;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Gateway start(ServiceCall call, ServiceRegistry serviceRegistry) {
    loopResources =
        LoopResources.create(id + ":" + port, LoopResources.DEFAULT_IO_WORKER_COUNT, true);

    try {
      server =
          HttpServer.create()
              .runOn(loopResources)
              .bindAddress(() -> new InetSocketAddress(port))
              .doOnConnection(
                  connection -> {
                    if (corsEnabled) {
                      connection.addHandlerLast(new CorsHandler(corsConfigBuilder.build()));
                    }
                  })
              .handle(
                  new HttpGatewayAcceptor(
                      callFactory.apply(call), serviceRegistry, errorMapper, authenticator))
              .httpFormDecoder(
                  builder -> {
                    if (formDecoderBuilder != null) {
                      formDecoderBuilder.accept(builder);
                    }
                  })
              .bind()
              .toFuture()
              .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return this;
  }

  @Override
  public Address address() {
    InetSocketAddress address = (InetSocketAddress) server.address();
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public void stop() {
    shutdownServer(server);
    shutdownLoopResources(loopResources);
  }

  private void shutdownServer(DisposableServer server) {
    if (server != null) {
      server.dispose();
    }
  }

  private void shutdownLoopResources(LoopResources loopResources) {
    if (loopResources != null) {
      loopResources.dispose();
    }
  }

  public static class Builder {

    private String id = "http@" + Integer.toHexString(hashCode());
    private int port;
    private Function<ServiceCall, ServiceCall> callFactory = call -> call;
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private HttpGatewayAuthenticator authenticator = HttpGatewayAuthenticator.DEFAULT_INSTANCE;
    private boolean corsEnabled = false;
    private CorsConfigBuilder corsConfigBuilder =
        CorsConfigBuilder.forAnyOrigin()
            .allowedRequestMethods(SUPPORTED_METHODS.toArray(HttpMethod[]::new))
            .allowNullOrigin()
            .allowedRequestHeaders("*")
            .exposeHeaders("*")
            .maxAge(3600);
    private Consumer<HttpServerFormDecoderProvider.Builder> formDecoderBuilder =
        builder -> builder.maxSize(100 * 1024 * 1024);

    private Builder() {}

    public String id() {
      return id;
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public int port() {
      return port;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder serviceCall(Function<ServiceCall, ServiceCall> operator) {
      callFactory = callFactory.andThen(operator);
      return this;
    }

    public ServiceProviderErrorMapper errorMapper() {
      return errorMapper;
    }

    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public HttpGatewayAuthenticator authenticator() {
      return authenticator;
    }

    public Builder authenticator(HttpGatewayAuthenticator authenticator) {
      this.authenticator = authenticator;
      return this;
    }

    public boolean corsEnabled() {
      return corsEnabled;
    }

    public Builder corsEnabled(boolean corsEnabled) {
      this.corsEnabled = corsEnabled;
      return this;
    }

    public CorsConfigBuilder corsConfigBuilder() {
      return corsConfigBuilder;
    }

    public Builder corsConfigBuilder(CorsConfigBuilder corsConfigBuilder) {
      this.corsConfigBuilder = corsConfigBuilder;
      return this;
    }

    public Builder corsConfigBuilder(Consumer<CorsConfigBuilder> consumer) {
      consumer.accept(corsConfigBuilder);
      return this;
    }

    public Consumer<HttpServerFormDecoderProvider.Builder> formDecoderBuilder() {
      return formDecoderBuilder;
    }

    public Builder formDecoderBuilder(Consumer<HttpServerFormDecoderProvider.Builder> consumer) {
      this.formDecoderBuilder = consumer;
      return this;
    }

    public HttpGateway build() {
      return new HttpGateway(this);
    }
  }
}
