package io.scalecube.services.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.methods.ServiceRoleDefinition;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import reactor.core.publisher.Mono;

public class ServiceRolesProcessorTest {

  @Test
  void processSuccessfully() {
    final var serviceRolesProcessor = mock(ServiceRolesProcessor.class);

    final var expectedServiceRoles =
        List.of(
            new ServiceRoleDefinition("admin", Set.of("*")),
            new ServiceRoleDefinition("user", Set.of("read")),
            new ServiceRoleDefinition("foo", Set.of("read", "write", "delete")));

    //noinspection unused
    try (final var microservices =
        Microservices.start(
            new Context()
                .serviceRolesProcessor(serviceRolesProcessor)
                .services(new Service1Impl(), new Service2Impl(), new Service3Impl()))) {
      verify(serviceRolesProcessor)
          .process(
              ArgumentMatchers.argThat(
                  serviceRoles -> {
                    assertNotNull(serviceRoles, "serviceRoles");
                    assertEquals(
                        expectedServiceRoles.size(), serviceRoles.size(), "serviceRoles.size");
                    for (var role : expectedServiceRoles) {
                      assertThat(serviceRoles, hasItem(role));
                    }
                    return true;
                  }));
    }
  }

  @Service
  private interface Service1 {

    @ServiceMethod
    Mono<Void> hello();
  }

  @AllowedRole(
      name = "admin",
      permissions = {"*"})
  public static class Service1Impl implements Service1 {

    @Override
    public Mono<Void> hello() {
      return null;
    }
  }

  @Service
  private interface Service2 {

    @ServiceMethod
    Mono<Void> hello();
  }

  @Service
  public static class Service2Impl implements Service2 {

    @AllowedRole(
        name = "user",
        permissions = {"read"})
    @Override
    public Mono<Void> hello() {
      return null;
    }
  }

  @Service
  private interface Service3 {

    @ServiceMethod
    Mono<Void> hello1();

    @ServiceMethod
    Mono<Void> hello2();

    @ServiceMethod
    Mono<Void> hello3();

    @ServiceMethod
    Mono<Void> hello4();
  }

  @AllowedRole(
      name = "foo",
      permissions = {"read"})
  public static class Service3Impl implements Service3 {

    @Override
    public Mono<Void> hello1() {
      return null;
    }

    @Override
    public Mono<Void> hello2() {
      return null;
    }

    @AllowedRole(
        name = "foo",
        permissions = {"write"})
    @Override
    public Mono<Void> hello3() {
      return null;
    }

    @AllowedRole(
        name = "foo",
        permissions = {"delete"})
    @Override
    public Mono<Void> hello4() {
      return null;
    }
  }
}
