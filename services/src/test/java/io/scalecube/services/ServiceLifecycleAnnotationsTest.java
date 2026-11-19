package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.BeforeDestroy;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.ConcurrentModificationException;
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
  void testStartFailsWithConcurrentModificationThenBeforeDestroy() {
    final var exception = new ConcurrentModificationException("gateway start failed");

    final var ex =
        assertThrows(
            ConcurrentModificationException.class,
            () ->
                Microservices.start(
                    new Context()
                        .gateway(
                            () ->
                                new Gateway() {
                                  @Override
                                  public String id() {
                                    return "failing";
                                  }

                                  @Override
                                  public Address address() {
                                    return Address.NULL_ADDRESS;
                                  }

                                  @Override
                                  public Gateway start(
                                      ServiceCall call, ServiceRegistry serviceRegistry) {
                                    throw exception;
                                  }

                                  @Override
                                  public void stop() {}
                                })
                        .services(
                            ServiceInfo.fromServiceInstance(
                                    new TestService() {
                                      @BeforeDestroy
                                      void cleanup() {
                                        beforeDestroy.invoke();
                                      }
                                    })
                                .build())));

    assertSame(exception, ex);
    // startup failed, so the partly started instance was cleaned up
    verify(beforeDestroy, times(1)).invoke();
  }

  @Test
  void testStartWithConcludedContextKeepsFirstInstance() {
    final var context =
        new Context()
            .services(
                ServiceInfo.fromServiceInstance(
                        new TestService() {
                          @BeforeDestroy
                          void cleanup() {
                            beforeDestroy.invoke();
                          }
                        })
                    .build());

    try (Microservices first = Microservices.start(context)) {
      assertThrows(ConcurrentModificationException.class, () -> Microservices.start(context));
      // the context belongs to the first instance, which must not be closed
      verify(beforeDestroy, times(0)).invoke();
    }
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
      assertSame(exception, getRootCause(ex));
    }

    verify(afterConstruct, times(1)).invoke();
    verify(beforeDestroy, times(1)).invoke();
  }

  @Test
  void testAfterConstructWithUnresolvableParameterFailsStart() {
    final var context =
        new Context()
            .services(
                new TestService() {
                  @AfterConstruct
                  void init(String unresolvable) {
                    afterConstruct.invoke();
                  }
                });

    final var ex = assertThrows(Exception.class, () -> Microservices.start(context));

    assertInstanceOf(IllegalArgumentException.class, getRootCause(ex));
    verify(afterConstruct, times(0)).invoke();
  }

  private static Throwable getRootCause(Throwable t) {
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t;
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
