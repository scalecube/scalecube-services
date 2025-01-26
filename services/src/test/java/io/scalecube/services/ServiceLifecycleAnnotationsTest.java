package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.annotations.Service;
import org.apache.logging.log4j.core.util.Throwables;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ServiceLifecycleAnnotationsTest {

  private final AfterConstructHandle afterConstruct = Mockito.mock(AfterConstructHandle.class);
  private final BeforeDestroyHandle beforeDestroy = Mockito.mock(BeforeDestroyHandle.class);

  @Test
  void testAfterConstructThenBeforeDestroy() {
    //noinspection EmptyTryBlock,unused
    try (Microservices microservices =
        Microservices.start(
            new Context()
                .services(
                    ServiceInfo.fromServiceInstance(
                            new TestService() {
                              @AfterConstruct
                              void init() {
                                afterConstruct.invoke();
                              }
                            })
                        .build(),
                    ServiceInfo.fromServiceInstance(
                            new TestService() {
                              @BeforeDestroy
                              void cleanup() {
                                beforeDestroy.invoke();
                              }
                            })
                        .build()))) {}

    verify(afterConstruct, times(1)).invoke();
    verify(beforeDestroy, times(1)).invoke();
  }

  @Test
  void testAfterConstructFailsThenBeforeDestroy() {
    final RuntimeException exception = new RuntimeException("AfterConstruct failed");
    doThrow(exception).when(afterConstruct).invoke();

    //noinspection EmptyTryBlock,unused
    try (Microservices microservices =
        Microservices.start(
            new Context()
                .services(
                    ServiceInfo.fromServiceInstance(
                            new TestService() {
                              @AfterConstruct
                              void init() {
                                afterConstruct.invoke();
                              }
                            })
                        .build(),
                    ServiceInfo.fromServiceInstance(
                            new TestService() {
                              @BeforeDestroy
                              void cleanup() {
                                beforeDestroy.invoke();
                              }
                            })
                        .build()))) {
    } catch (Exception ex) {
      assertSame(exception, Throwables.getRootCause(ex));
    }

    verify(afterConstruct, times(1)).invoke();
    verify(beforeDestroy, times(1)).invoke();
  }

  @Service("service")
  interface TestService {}

  interface AfterConstructHandle {

    void invoke();
  }

  interface BeforeDestroyHandle {

    void invoke();
  }
}
