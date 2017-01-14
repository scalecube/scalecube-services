package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices.Builder;
import io.scalecube.services.Microservices.DispatcherContext;
import io.scalecube.services.Microservices.ProxyContext;
import io.scalecube.services.routing.RoundRobinServiceRouter;

import org.junit.Test;

public class MicroservicesTest {

  @Test
  public void test_microservices_config() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    assertTrue(servicesConfig.services().isEmpty());
    assertTrue(micro.services().isEmpty());
    micro.shutdown();
  }

  @Test
  public void test_microservices_dispatcher_router() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    DispatcherContext dispatcher = micro.dispatcher();
    dispatcher.router(RoundRobinServiceRouter.class);
    assertTrue(dispatcher.router().equals(RoundRobinServiceRouter.class));
    micro.shutdown();
  }

  @Test
  public void test_microservices_proxy_router() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    ProxyContext proxy = micro.proxy();

    assertTrue(proxy.router().equals(RoundRobinServiceRouter.class));
    micro.shutdown();
  }
}
