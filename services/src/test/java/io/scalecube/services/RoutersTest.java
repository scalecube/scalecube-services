package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.routing.RandomServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.services.routing.Routers;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.function.UnaryOperator;

public class RoutersTest extends BaseTest {

  @Test
  public void test_router_factory() {
    RouterFactory factory = new RouterFactory(null);
    Router router = factory.getRouter(RandomServiceRouter.class);
    assertTrue(router != null);

    // dummy router will always throw exception thus cannot be created.
    Router dummy = factory.getRouter(DummyRouter.class);
    assertTrue(dummy == null);

  }

  @Service
  @FunctionalInterface
  interface StringUnaryOperator extends UnaryOperator<String> {
  };

  @Test
  public void test_tag_router() throws InterruptedException, ExecutionException {


    StringUnaryOperator upperCase = String::toUpperCase;
    StringUnaryOperator loerCase = String::toLowerCase;
    Microservices ms = Microservices.builder()
        .services()
        .service(upperCase).tag("to", "upper").add()
        .service(loerCase).tag("to", "lower").add()
        .build()
        .build();



    Microservices ms2 = Microservices.builder().seeds(ms.cluster().address()).build();
    StringUnaryOperator uppercaseProxy =
        ms2.proxy().router(Routers.withTag("to", "upper")).api(StringUnaryOperator.class).create();
    StringUnaryOperator lowercaseProxy =
        ms2.proxy().router(Routers.withTag("to", "lower")).api(StringUnaryOperator.class).create();

    assertEquals("ABC", uppercaseProxy.apply("abC"));
    assertEquals("abc", lowercaseProxy.apply("abC"));
    ms.shutdown().get();

  }

}
