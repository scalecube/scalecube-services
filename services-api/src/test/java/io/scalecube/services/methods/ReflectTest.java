package io.scalecube.services.methods;

import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.api.ServiceMessage;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReflectTest {

  /**
   * Test communication mode.
   *
   * @param methodName method name
   * @param expectedMode expected mode
   */
  @ParameterizedTest
  @MethodSource("argsCommunicationModeProvider")
  public void testCommunicationMode(String methodName, CommunicationMode expectedMode) {
    // Given:
    Method m =
        Arrays.stream(TestService.class.getMethods())
            .filter(meth -> meth.getName().equals(methodName))
            .findFirst()
            .get();
    // When:
    CommunicationMode communicationMode = Reflect.communicationMode(m);
    // Then:
    Assertions.assertEquals(expectedMode, communicationMode, "Invalid communicationMode");
  }

  static Stream<Arguments> argsCommunicationModeProvider() {
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
        Arrays.stream(ReflectTest.TestService.class.getMethods())
            .filter(meth -> meth.getName().equals(methodName))
            .findFirst()
            .get();
    // When:
    boolean actual = Reflect.isRequestTypeServiceMessage(method);
    // Then:
    Assertions.assertEquals(
        expect,
        actual,
        String.format("isRequestTypeServiceMessage(%s) should be %b", methodName, expect));
  }

  static Stream<Arguments> argsIsRequestTypeServiceMessage() {
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

  private interface TestService {
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
    public String name();
  }

  private class ServiceImpl implements SimpleService {
    @Override
    public String name() {
      return "duke";
    }
  }

  private class SubServiceImpl extends ServiceImpl {}

  @Test
  public void testSubServiceInterfaces() {

    // When:
    Stream<Class<?>> interfaces = Reflect.serviceInterfaces(new SubServiceImpl());
    // Then:
    Assertions.assertEquals(
        1, interfaces.count(), "serviceInterfaces(..) should detect interfaces in SubServiceImpl");
  }
}
