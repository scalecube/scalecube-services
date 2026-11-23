package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

/** {@code @BeforeDestroy} runs after inbound traffic stops, while outbound calls still work. */
class ShutdownOrderTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Test
  void beforeDestroyRunsAfterInboundStopsAndBeforeOutboundCloses() {
    final Microservices other =
        Microservices.start(
            new Context().transport(RSocketServiceTransport::new).services(new OtherServiceImpl()));
    final LoopResources loopResources = LoopResources.create("shutdown-order-test", 1, true);
    final NodeServiceImpl nodeService = new NodeServiceImpl();
    try {
      final Microservices node =
          Microservices.start(
              new Context().transport(RSocketServiceTransport::new).services(nodeService));

      // calls into the node from outside, and from the node out to the other node
      nodeService.inboundCall = remoteCall(node.serviceAddress(), loopResources);
      nodeService.outboundCall =
          node.call()
              .router(StaticAddressRouter.forService(other.serviceAddress(), "other").build());
      assertEquals("node", call(nodeService.inboundCall, "v1/node/ping"), "inbound while running");

      node.close();

      assertEquals("unreachable", nodeService.inboundDuringBeforeDestroy, "inbound in hook");
      assertEquals("other", nodeService.outboundDuringBeforeDestroy, "outbound in hook");
    } finally {
      other.close();
      loopResources.dispose();
    }
  }

  private static ServiceCall remoteCall(Address address, LoopResources loopResources) {
    return new ServiceCall()
        .transport(
            new RSocketClientTransport(
                HeadersCodec.DEFAULT_INSTANCE,
                DataCodec.getAllInstances(),
                RSocketClientTransportFactory.websocket().apply(loopResources),
                null,
                null,
                0,
                0))
        .router(StaticAddressRouter.forService(address, "node").build());
  }

  private static String call(ServiceCall serviceCall, String qualifier) {
    return serviceCall
        .requestOne(ServiceMessage.builder().qualifier(qualifier).build(), String.class)
        .map(ServiceMessage::<String>data)
        .onErrorReturn("unreachable")
        .block(TIMEOUT);
  }

  @Service("v1/node")
  public interface NodeService {

    @ServiceMethod
    Mono<String> ping();
  }

  public static class NodeServiceImpl implements NodeService {

    volatile ServiceCall inboundCall;
    volatile ServiceCall outboundCall;
    volatile String inboundDuringBeforeDestroy;
    volatile String outboundDuringBeforeDestroy;

    @Override
    public Mono<String> ping() {
      return Mono.just("node");
    }

    @BeforeDestroy
    void beforeDestroy() {
      inboundDuringBeforeDestroy = call(inboundCall, "v1/node/ping");
      outboundDuringBeforeDestroy = call(outboundCall, "v1/other/ping");
    }
  }

  @Service("v1/other")
  public interface OtherService {

    @ServiceMethod
    Mono<String> ping();
  }

  public static class OtherServiceImpl implements OtherService {

    @Override
    public Mono<String> ping() {
      return Mono.just("other");
    }
  }
}
