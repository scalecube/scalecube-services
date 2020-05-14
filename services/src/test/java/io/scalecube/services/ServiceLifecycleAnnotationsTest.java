package io.scalecube.services;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.annotations.Service;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ServiceLifecycleAnnotationsTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Test
  void testAfterConstructThenBeforeDestroy() throws Exception {
    final CountDownLatch afterConstruct = new CountDownLatch(1);
    final CountDownLatch beforeDestroy = new CountDownLatch(1);

    Microservices microservices =
        Microservices.builder()
            .services(
                ServiceInfo.fromServiceInstance(
                        new TestService() {
                          @AfterConstruct
                          void init() {
                            afterConstruct.countDown();
                          }
                        })
                    .build(),
                ServiceInfo.fromServiceInstance(
                        new TestService() {
                          @BeforeDestroy
                          void cleanup() {
                            beforeDestroy.countDown();
                          }
                        })
                    .build())
            .startAwait();

    afterConstruct.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    Assertions.assertEquals(0, afterConstruct.getCount());

    microservices.shutdown().block();

    beforeDestroy.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    Assertions.assertEquals(0, beforeDestroy.getCount());
  }

  @Test
  void testAfterConstructFailsThenBeforeDestroy() throws Exception {
    final CountDownLatch beforeDestroy = new CountDownLatch(1);

    Assertions.assertThrows(
        Exception.class,
        () ->
            Microservices.builder()
                .services(
                    ServiceInfo.fromServiceInstance(
                            new TestService() {
                              @AfterConstruct
                              void init() {
                                throw new RuntimeException("AfterConstruct failed");
                              }
                            })
                        .build(),
                    ServiceInfo.fromServiceInstance(
                            new TestService() {
                              @BeforeDestroy
                              void init() {
                                beforeDestroy.countDown();
                              }
                            })
                        .build())
                .startAwait());

    beforeDestroy.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    Assertions.assertEquals(0, beforeDestroy.getCount());
  }

  @Service("service")
  interface TestService {}
}
