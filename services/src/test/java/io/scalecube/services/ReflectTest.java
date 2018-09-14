package io.scalecube.services;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
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
  @MethodSource("argsProvider")
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

  static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of("fireAndForget", FIRE_AND_FORGET),
        Arguments.of("requestResponse", REQUEST_RESPONSE),
        Arguments.of("requestStream", REQUEST_STREAM),
        Arguments.of("requestChannel", REQUEST_CHANNEL));
  }

  private interface TestService {
    void fireAndForget(Integer i);

    Mono<Void> emptyResponse(Integer i);

    Mono<Integer> requestResponse(Integer i);

    Flux<Integer> requestStream(Integer i);

    Flux<Integer> requestChannel(Flux<Integer> i);
  }
}
