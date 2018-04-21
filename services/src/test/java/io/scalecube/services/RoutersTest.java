package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.routing.RandomServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

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


}
