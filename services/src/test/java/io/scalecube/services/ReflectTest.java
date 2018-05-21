package io.scalecube.services;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RunWith(Parameterized.class)
public class ReflectTest {
  @Parameterized.Parameters(name = "method:{0}, mode:{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"fireAndForget", FIRE_AND_FORGET},
        {"emptyResponse", REQUEST_RESPONSE},
        {"requestResponse", REQUEST_RESPONSE},
        {"requestStream", REQUEST_STREAM},
        {"requestChannel", REQUEST_CHANNEL},
    });
  }

  private String methodName;
  private CommunicationMode expectedMode;

  public ReflectTest(String methodName, CommunicationMode expectedMode) {
    this.methodName = methodName;
    this.expectedMode = expectedMode;
  }

  @Test
  public void testCommunicationMode() {
    // Given:
    Method m = Arrays.stream(TestService.class.getMethods()).filter(meth -> meth.getName().equals(methodName))
        .findFirst().get();
    // When:
    CommunicationMode communicationMode = Reflect.communicationMode(m);
    // Then:
    Assert.assertEquals("Invalid communicationMode", expectedMode, communicationMode);
  }

  private interface TestService {
    void fireAndForget(Integer i);
    
    Mono<Void> emptyResponse(Integer i);
    
    Mono<Integer> requestResponse(Integer i);

    Flux<Integer> requestStream(Integer i);

    Flux<Integer> requestChannel(Flux<Integer> i);
  }
}
