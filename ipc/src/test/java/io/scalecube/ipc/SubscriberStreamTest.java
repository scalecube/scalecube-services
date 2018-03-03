package io.scalecube.ipc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.Observable;
import rx.Subscription;
import rx.observables.BlockingObservable;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriberStreamTest {

  private Address echoAddress;
  private Address manyEchoAddress;
  private ListeningServerStream echoServerStream;
  private ListeningServerStream manyEchoServerStream;
  private SubscriberStream subscriberStream;

  @Before
  public void setUp() throws Exception {
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().withListenAddress("localhost");
    subscriberStream = SubscriberStream.newSubscriberStream();

    // echo service infra
    echoServerStream = serverStream.bind();
    echoAddress = echoServerStream.listenBind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
    echoServerStream.listenMessageReadSuccess().subscribe(echoServerStream::send);

    // many echo service infra
    manyEchoServerStream = serverStream.bind();
    manyEchoAddress = manyEchoServerStream.listenBind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
    manyEchoServerStream.listenMessageReadSuccess().subscribe(message -> {
      manyEchoServerStream.send(message);
      manyEchoServerStream.send(message);
      manyEchoServerStream.send(message);
    });
  }

  @After
  public void cleanUp() throws Exception {
    subscriberStream.close();
    echoServerStream.close();
    manyEchoServerStream.close();
    echoAddress = echoServerStream.listenUnbind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
    manyEchoAddress = manyEchoServerStream.listenUnbind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testUnsubscribeBehavior() throws Exception {
    Address dummyAddress = Address.from("localhost:7070");
    ServiceMessage message = ServiceMessage.withQualifier("q/hello").build();
    Observable<ServiceMessage> observable = subscriberStream.listenOnNext(dummyAddress, message);
    Subscription subscription = observable.subscribe();
    subscription.unsubscribe();
    assertTrue(subscription.isUnsubscribed());
  }

  @Test
  public void testReoccuringOperation() throws Exception {
    AtomicInteger i = new AtomicInteger();
    int n = 2;
    ServiceMessage message = ServiceMessage.withQualifier("q/repeat").build();
    Observable<ServiceMessage> observable =
        subscriberStream
            .listenOnNext(echoAddress, message)
            .doOnNext(message1 -> {
              if (i.incrementAndGet() < n)
                throw new RuntimeException("hola");
            })
            .retry(n);
    assertEquals(message, observable.toBlocking().first());
  }

  @Test
  public void testListenOnNextWithTimeout() throws Exception {
    Address dummyAddress = Address.from("localhost:7070");
    ServiceMessage message = ServiceMessage.withQualifier("q/hello").build();
    Observable<ServiceMessage> observable = subscriberStream
        .listenOnNext(dummyAddress, message)
        .timeout(1, TimeUnit.SECONDS);
    try {
      assertEquals(message, observable.toBlocking().first());
      fail("Expected TimeoutException");
    } catch (Exception e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testSingleRequestResponse() throws Exception {
    ServiceMessage message = ServiceMessage.withQualifier("q/hello").build();
    Observable<ServiceMessage> observable = subscriberStream.listenOnNext(echoAddress, message);
    assertEquals(message, observable.toBlocking().first());
  }

  @Test
  public void testSingleRequestSeveralResponses() throws Exception {
    ServiceMessage message = ServiceMessage.withQualifier("q/hello").build();
    Observable<ServiceMessage> observable = subscriberStream.listenOnNext(manyEchoAddress, message);
    Iterator<ServiceMessage> iterator = observable.toBlocking().getIterator();
    assertThat(iterator.next(), is(message));
    assertThat(iterator.next(), is(message));
    assertThat(iterator.next(), is(message));
  }

  @Test
  public void testMergeSeveralReplies() throws Exception {
    ServiceMessage message1 = ServiceMessage.withQualifier("1").build();
    ServiceMessage message2 = ServiceMessage.withQualifier("2").build();
    ServiceMessage message3 = ServiceMessage.withQualifier("3").build();
    Observable<ServiceMessage> observable1 = subscriberStream.listenOnNext(echoAddress, message1);
    Observable<ServiceMessage> observable2 = subscriberStream.listenOnNext(echoAddress, message2);
    Observable<ServiceMessage> observable3 = subscriberStream.listenOnNext(echoAddress, message3);
    BlockingObservable<ServiceMessage> observable =
        Observable.merge(observable1, observable2, observable3).toBlocking();
    Iterator<ServiceMessage> iterator = observable.getIterator();
    assertThat(iterator.next(), anyOf(is(message1), is(message2), is(message3)));
    assertThat(iterator.next(), anyOf(is(message1), is(message2), is(message3)));
    assertThat(iterator.next(), anyOf(is(message1), is(message2), is(message3)));
  }
}
