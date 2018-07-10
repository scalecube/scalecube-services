package io.scalecube.gateway.http;

import io.scalecube.services.Microservices;

import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.tcp.BlockingNettyContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Gateway implementation over HTTP protocol.
 */
public class GatewayHttpServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpServer.class);

  private static final int DEFAULT_PORT = 8080;

  private final InetSocketAddress address;
  private final GatewayHttpAcceptor httpAcceptor;

  private HttpServer httpServer;
  private BlockingNettyContext context;

  /**
   * Creates instance of gateway is listening on {@value DEFAULT_PORT} port.
   *
   * @param microservices instance of {@link Microservices}
   */
  public GatewayHttpServer(Microservices microservices) {
    this(microservices, DEFAULT_PORT);
  }

  /**
   * Creates instance of gateway is listening on port which is passed as input parameter.
   *
   * @param microservices instance of {@link Microservices}
   * @param port gateway port
   */
  public GatewayHttpServer(Microservices microservices, int port) {
    this(microservices, new InetSocketAddress(port));
  }

  /**
   * Creates instance of gateway listening on {@link InetSocketAddress} which is passed as input parameter.
   *
   * @param microservices instance of {@link Microservices}
   * @param listenAddress gateway IP socket address
   */
  public GatewayHttpServer(Microservices microservices, InetSocketAddress listenAddress) {
    address = listenAddress;
    httpServer = HttpServer.builder().listenAddress(address).build();
    httpAcceptor = new GatewayHttpAcceptor(microservices.call().create());
  }

  /**
   * Starts the gateway on configured IP socket address.
   *
   * @return IP socket address on which gateway is listening to requests
   */
  public InetSocketAddress start() {
    LOGGER.info("Starting gateway on {}", address);

    context = httpServer.start(httpAcceptor);

    LOGGER.info("Gateway has been started successfully on {}", address);

    return context.getContext().address();
  }

  /**
   * Stops the gateway.
   */
  public void stop() {
    if (context == null) {
      LOGGER.warn("Gateway is not started");
      return;
    }

    LOGGER.info("Stopping gateway...");

    context.shutdown();

    LOGGER.info("Gateway has been stopped successfully");
  }

  public InetSocketAddress address() {
    return address;
  }

}
