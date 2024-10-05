package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.Subscriber;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

public class ServiceDiscoverySubscriberTest extends BaseTest {

  @Test
  void testRegisterNonDiscoveryCoreSubscriber() {
    final NonDiscoverySubscriber1 discoverySubscriber1 = spy(new NonDiscoverySubscriber1());
    final NonDiscoverySubscriber2 discoverySubscriber2 = spy(new NonDiscoverySubscriber2());

    Microservices.start(new Context().services(discoverySubscriber1, discoverySubscriber2));

    verifyNoInteractions(discoverySubscriber1, discoverySubscriber2);
  }

  @Test
  void testRegisterNotMatchingTypeDiscoveryCoreSubscriber() {
    final NotMatchingTypeDiscoverySubscriber discoverySubscriber =
        spy(new NotMatchingTypeDiscoverySubscriber());

    Microservices.start(new Context().services(discoverySubscriber));

    verifyNoInteractions(discoverySubscriber);
  }

  @Test
  void testRegisterDiscoveryCoreSubscriber() {
    final AtomicReference<Subscription> subscriptionReference = new AtomicReference<>();
    final NormalDiscoverySubscriber normalDiscoverySubscriber =
        new NormalDiscoverySubscriber(subscriptionReference);

    Microservices.start(new Context().services(normalDiscoverySubscriber));

    assertNotNull(subscriptionReference.get(), "subscription");
  }

  private static class SomeType {}

  private static class NonDiscoverySubscriber1 extends BaseSubscriber {}

  private static class NonDiscoverySubscriber2 extends BaseSubscriber<SomeType> {}

  @Subscriber
  private static class NotMatchingTypeDiscoverySubscriber
      extends BaseSubscriber<ServiceDiscoveryEvent> {}

  @Subscriber(ServiceDiscoveryEvent.class)
  private static class NormalDiscoverySubscriber extends BaseSubscriber<ServiceDiscoveryEvent> {

    private final AtomicReference<Subscription> subscriptionReference;

    private NormalDiscoverySubscriber(AtomicReference<Subscription> subscriptionReference) {
      this.subscriptionReference = subscriptionReference;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      subscriptionReference.set(subscription);
    }
  }
}
