package io.scalecube.services.methods;

import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.AllowedRole;
import io.scalecube.services.auth.AllowedRoles;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReflectTest {

  @ParameterizedTest
  @MethodSource("argsCommunicationModeProvider")
  public void testCommunicationMode(String methodName, CommunicationMode expectedMode) {
    // Given:
    Method m =
        Arrays.stream(JustService.class.getMethods())
            .filter(meth -> meth.getName().equals(methodName))
            .findFirst()
            .get();
    // When:
    CommunicationMode communicationMode = Reflect.communicationMode(m);
    // Then:
    assertEquals(expectedMode, communicationMode, "Invalid communicationMode");
  }

  private static Stream<Arguments> argsCommunicationModeProvider() {
    return Stream.of(
        Arguments.of("fireAndForget", REQUEST_RESPONSE),
        Arguments.of("emptyResponse", REQUEST_RESPONSE),
        Arguments.of("requestResponse", REQUEST_RESPONSE),
        Arguments.of("requestStream", REQUEST_STREAM),
        Arguments.of("requestChannel", REQUEST_CHANNEL),
        Arguments.of("fireAndForgetMessage", REQUEST_RESPONSE),
        Arguments.of("emptyResponseMessage", REQUEST_RESPONSE),
        Arguments.of("requestResponseMessage", REQUEST_RESPONSE),
        Arguments.of("requestStreamMessage", REQUEST_STREAM),
        Arguments.of("requestChannelMessage", REQUEST_CHANNEL));
  }

  @ParameterizedTest
  @MethodSource("argsIsRequestTypeServiceMessage")
  public void testIsRequestTypeServiceMessage(String methodName, boolean expect) {
    // Given:
    Method method =
        Arrays.stream(JustService.class.getMethods())
            .filter(meth -> meth.getName().equals(methodName))
            .findFirst()
            .get();
    // When:
    boolean actual = Reflect.isRequestTypeServiceMessage(method);
    // Then:
    assertEquals(
        expect,
        actual,
        String.format("isRequestTypeServiceMessage(%s) should be %b", methodName, expect));
  }

  private static Stream<Arguments> argsIsRequestTypeServiceMessage() {
    return Stream.of(
        Arguments.of("fireAndForget", false),
        Arguments.of("emptyResponse", false),
        Arguments.of("requestResponse", false),
        Arguments.of("requestStream", false),
        Arguments.of("requestChannel", false),
        Arguments.of("fireAndForgetMessage", true),
        Arguments.of("emptyResponseMessage", true),
        Arguments.of("requestResponseMessage", true),
        Arguments.of("requestStreamMessage", true),
        Arguments.of("requestChannelMessage", true));
  }

  @Test
  public void testSubServiceInterfaces() {

    // When:
    Stream<Class<?>> interfaces = Reflect.serviceInterfaces(new SubServiceImpl());
    // Then:
    assertEquals(
        1, interfaces.count(), "serviceInterfaces(..) should detect interfaces in SubServiceImpl");
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("testServiceRolesMethodSource")
  public void testServiceRoles(String test, SuccessArgs args) throws Exception {
    final var method = args.serviceInstance.getClass().getMethod("hello");
    final var serviceRoles = Reflect.serviceRoles(method);
    assertNotNull(serviceRoles, "serviceRoles");
    assertEquals(args.list.size(), serviceRoles.size(), "serviceRoles.size");
    for (var expectedServiceRole : args.list) {
      assertThat(serviceRoles, hasItem(expectedServiceRole));
    }
  }

  private record SuccessArgs(Object serviceInstance, List<ServiceRoleDefinition> list) {}

  private static Stream<Arguments> testServiceRolesMethodSource() {
    return Stream.of(
        Arguments.of(
            "@AllowedRoles declared on class",
            new SuccessArgs(
                new ServiceRoleService1Impl(),
                List.of(
                    new ServiceRoleDefinition("user", Set.of("read", "write")),
                    new ServiceRoleDefinition("admin", Set.of("*"))))),
        Arguments.of(
            "Repeatable @AllowedRole declared on class",
            new SuccessArgs(
                new ServiceRoleService2Impl(),
                List.of(
                    new ServiceRoleDefinition("user", Set.of("read", "write")),
                    new ServiceRoleDefinition("admin", Set.of("*"))))),
        Arguments.of(
            "@AllowedRoles declared on method",
            new SuccessArgs(
                new ServiceRoleService3Impl(),
                List.of(
                    new ServiceRoleDefinition("user", Set.of("read", "write")),
                    new ServiceRoleDefinition("admin", Set.of("*"))))),
        Arguments.of(
            "Repeatable @AllowedRole declared on method",
            new SuccessArgs(
                new ServiceRoleService4Impl(),
                List.of(
                    new ServiceRoleDefinition("user", Set.of("read", "write")),
                    new ServiceRoleDefinition("admin", Set.of("*"))))),
        Arguments.of(
            "No @AllowedRoles/@AllowedRole annotations declared anywhere",
            new SuccessArgs(new ServiceRoleService5Impl(), List.of())));
  }

  private interface JustService {

    void fireAndForget(Integer i);

    Mono<Void> emptyResponse(Integer i);

    Mono<Integer> requestResponse(Integer i);

    Flux<Integer> requestStream(Integer i);

    Flux<Integer> requestChannel(Flux<Integer> i);

    void fireAndForgetMessage(ServiceMessage sm);

    Mono<Void> emptyResponseMessage(ServiceMessage sm);

    Mono<ServiceMessage> requestResponseMessage(ServiceMessage sm);

    Flux<ServiceMessage> requestStreamMessage(ServiceMessage sm);

    Flux<ServiceMessage> requestChannelMessage(Flux<ServiceMessage> sm);
  }

  @Service
  private interface SimpleService {

    String name();
  }

  private static class ServiceImpl implements SimpleService {

    @Override
    public String name() {
      return "duke";
    }
  }

  private static class SubServiceImpl extends ServiceImpl {}

  @Service
  private interface ServiceRoleService1 {

    @ServiceMethod
    Mono<Void> hello();
  }

  @AllowedRoles(
      value = {
        @AllowedRole(
            name = "user",
            permissions = {"read", "write"}),
        @AllowedRole(
            name = "admin",
            permissions = {"*"})
      })
  public static class ServiceRoleService1Impl implements ServiceRoleService1 {

    @Override
    public Mono<Void> hello() {
      return null;
    }
  }

  @Service
  private interface ServiceRoleService2 {

    @ServiceMethod
    Mono<Void> hello();
  }

  @AllowedRole(
      name = "user",
      permissions = {"read", "write"})
  @AllowedRole(
      name = "admin",
      permissions = {"*"})
  public static class ServiceRoleService2Impl implements ServiceRoleService2 {

    @Override
    public Mono<Void> hello() {
      return null;
    }
  }

  @Service
  private interface ServiceRoleService3 {

    @ServiceMethod
    Mono<Void> hello();
  }

  public static class ServiceRoleService3Impl implements ServiceRoleService3 {

    @AllowedRoles(
        value = {
          @AllowedRole(
              name = "user",
              permissions = {"read", "write"}),
          @AllowedRole(
              name = "admin",
              permissions = {"*"})
        })
    @Override
    public Mono<Void> hello() {
      return null;
    }
  }

  @Service
  private interface ServiceRoleService4 {

    @ServiceMethod
    Mono<Void> hello();
  }

  public static class ServiceRoleService4Impl implements ServiceRoleService4 {

    @AllowedRole(
        name = "user",
        permissions = {"read", "write"})
    @AllowedRole(
        name = "admin",
        permissions = {"*"})
    @Override
    public Mono<Void> hello() {
      return null;
    }
  }

  @Service
  private interface ServiceRoleService5 {

    @ServiceMethod
    Mono<Void> hello();
  }

  public static class ServiceRoleService5Impl implements ServiceRoleService5 {

    @Override
    public Mono<Void> hello() {
      return null;
    }
  }
}
