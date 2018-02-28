package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.ipc.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultEventStreamTest {

  private ServiceMessage message0 = ServiceMessage.withQualifier("ok").build();
  private ServiceMessage message1 = ServiceMessage.withQualifier("hola").build();

  private ChannelContext ctx0 = ChannelContext.create("sadsas78sd", Address.from("localhost:0"));
  private ChannelContext ctx1 = ChannelContext.create("asdfa7sd8f", Address.from("localhost:1"));

  private DefaultEventStream eventStream = new DefaultEventStream();

  @Test
  public void testChannelContextPostEvent() {
    List<Event> events = new ArrayList<>(2);
    eventStream.listen().subscribe(events::add);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    ctx0.postReadSuccess(message0);
    ctx1.postReadSuccess(message1);
    ctx1.postReadSuccess(message1);

    assertEquals(message0, events.get(0).getMessage().get());
    assertEquals(message0, events.get(1).getMessage().get());
    assertEquals(message1, events.get(2).getMessage().get());
    assertEquals(message1, events.get(3).getMessage().get());
  }

  @Test
  public void testChannelContextsAreIsolated() {
    List<Event> events = new ArrayList<>(2);
    eventStream.listen().subscribe(events::add);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    ctx1.close(); // at this point close ctx1

    // Post again via live context and expect msgs to come even some context closed
    ctx0.postReadSuccess(message0);
    ctx0.postReadSuccess(message0);

    assertEquals(message0, events.get(0).getMessage().get());
    assertEquals(message0, events.get(1).getMessage().get());
    assertEquals(message0, events.get(2).getMessage().get());
  }

  @Test
  public void testUnsubscribingChannelContextIsIsolated() {
    List<Event> events = new ArrayList<>(2);
    eventStream.listen().subscribe(events::add);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    ctx1.postReadSuccess(message1);

    assertEquals(message0, events.get(0).getMessage().get());
    assertEquals(message1, events.get(1).getMessage().get());

    ctx0.close();
    ctx1.close();

    // After two contexts closed business layer is not affected
    ChannelContext ctx2 = ChannelContext.create("d89asfads7f", Address.from("localhost:2"));
    eventStream.subscribe(ctx2);
    ctx2.postReadError(new RuntimeException("Can't decode incoming msg"));
    assertEquals(Topic.ReadError, events.get(2).getTopic());
  }

  @Test
  public void testChannelContextClosed() {
    AtomicBoolean eventSubjectClosed = new AtomicBoolean();
    AtomicBoolean channelContextClosed = new AtomicBoolean();
    // You can watch-out for close at major Observable<Event>
    ctx0.listen().subscribe(event -> {
    }, throwable -> {
    }, () -> eventSubjectClosed.set(true));
    // You can watch-out for close at Observable that was invented for exact reason
    ctx0.listenClose().subscribe(aVoid -> channelContextClosed.set(true));
    ctx0.close();
    assertTrue(eventSubjectClosed.get());
    assertTrue(channelContextClosed.get());
  }
}
