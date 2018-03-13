package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.routing.RandomServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.services.routing.Routers;
import io.scalecube.testlib.BaseTest;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RoutersTest extends BaseTest {



  private static final String TO_ = "to";
  private static final String LOWER_ = "lower";
  private static final String UPPER_ = "upper";

  private static final String TO = "TO";
  private static final String LOWER = "LOWER";
  private static final String UPPER = "UPPER";


  @Test
  public void test_router_factory() {
    RouterFactory factory = new RouterFactory(null);
    Router router = factory.getRouter(RandomServiceRouter.class);
    assertTrue(router != null);

    // dummy router will always throw exception thus cannot be created.
    Router dummy = factory.getRouter(DummyRouter.class);
    assertTrue(dummy == null);

  }



  @Test
  public void test_tag_router() throws InterruptedException, ExecutionException, TimeoutException {

    StringUnaryOperator upperCase = new StringUnaryOperatorImpl(String::toUpperCase);
    StringUnaryOperator lowerCase = new StringUnaryOperatorImpl(String::toLowerCase);

    Microservices ms1 = Microservices.builder()
        .services()
        .service(upperCase).tag(TO, UPPER).add()
        .build()
        .build();


    Microservices ms2 = Microservices.builder().seeds(ms1.cluster().address())
        .services()
        .service(lowerCase).tag(TO, LOWER).add()
        .build()
        .build();

    Microservices ms3 = Microservices.builder().seeds(ms2.cluster().address()).build();


    StringUnaryOperator uppercaseProxy =
        ms3.proxy().routing(Routers.withTag(TO, UPPER)).api(StringUnaryOperator.class).create();
    StringUnaryOperator lowercaseProxy =
        ms3.proxy().routing(Routers.withTag(TO, LOWER)).api(StringUnaryOperator.class).create();

    Random random = new Random(System.currentTimeMillis());

    for (int i = 0; i < 1000; i++) {

      String randomString = random.ints(20, 'A', 'z').mapToObj(ch -> Character.valueOf((char) ch))
          .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
      assertEquals(randomString.toUpperCase(), uppercaseProxy.apply(randomString).get(10, TimeUnit.MINUTES));
      assertEquals(randomString.toLowerCase(), lowercaseProxy.apply(randomString).get(10, TimeUnit.MINUTES));
    }
    CompletableFuture.allOf(
        ms1.shutdown(),
        ms2.shutdown(),
        ms3.shutdown()).get();

  }



  @Test
  public void test_tags_router() throws InterruptedException, ExecutionException, TimeoutException {

    StringUnaryOperator upperCase = new StringUnaryOperatorImpl(String::toUpperCase);
    StringUnaryOperator lowerCase = new StringUnaryOperatorImpl(String::toLowerCase);

    Microservices ms1 = Microservices.builder()
        .services()
        .service(upperCase).tag(TO, UPPER).tag(TO_, UPPER_).add()
        .build()
        .build();


    Microservices ms2 = Microservices.builder().seeds(ms1.cluster().address())
        .services()
        .service(lowerCase).tag(TO, LOWER).tag(TO_, LOWER_).add()
        .build()
        .build();

    Microservices ms3 = Microservices.builder().seeds(ms2.cluster().address()).build();

    Map<String, String> tagsToUpper = new HashMap<>();
    tagsToUpper.put(TO, UPPER);
    tagsToUpper.put(TO_, UPPER_);


    Map<String, String> tagsToLower = new HashMap<>();

    tagsToUpper.put(TO, LOWER);
    tagsToUpper.put(TO_, LOWER_);

    StringUnaryOperator uppercaseProxy =
        ms3.proxy().routing(Routers.withAllTags(tagsToUpper)).api(StringUnaryOperator.class).create();
    StringUnaryOperator lowercaseProxy =
        ms3.proxy().routing(Routers.withAllTags(tagsToLower)).api(StringUnaryOperator.class).create();


    Random random = new Random(System.currentTimeMillis());

    for (int i = 0; i < 1000; i++) {

      String randomString = random.ints(20, 'A', 'z').mapToObj(ch -> Character.valueOf((char) ch))
          .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
      assertEquals(randomString.toUpperCase(), uppercaseProxy.apply(randomString).get(10, TimeUnit.MINUTES));
      assertEquals(randomString.toLowerCase(), lowercaseProxy.apply(randomString).get(10, TimeUnit.MINUTES));
    }


    Map<String, String> tagsToNothing = new HashMap<>(tagsToUpper);
    tagsToNothing.put("MISSING", "WILLFAIL");
    StringUnaryOperator missingProxy =
        ms3.proxy().routing(Routers.withAllTags(tagsToNothing)).api(StringUnaryOperator.class).create();

    boolean failed;
    try {
      missingProxy.apply("A");
      failed = false;
    } catch (Exception ignoredException) {
      failed = true;
      assertThat(ignoredException, CoreMatchers.instanceOf(IllegalStateException.class));
      assertThat(ignoredException.getMessage(), CoreMatchers.containsString("No reachable member with such service:"));
    }
    assertTrue("negative routing test failed", failed);
    CompletableFuture.allOf(
        ms1.shutdown(),
        ms2.shutdown(),
        ms3.shutdown()).get();

  }



}
