package io.scalecube.streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Address;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import rx.subjects.PublishSubject;

public class ServerStreamTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ChannelContext channelContext;

  private ServerStream serverStream;

  @Before
  public void setUp() {
    channelContext = ChannelContext.create(Address.create("localhost", 0));
    serverStream = ServerStream.newServerStream();
    serverStream.subscribe(channelContext);
  }

  @Test
  public void testServerStreamListenMessageWithNoIdentity() throws Exception {
    String id = channelContext.getId();

    String[] subjects = new String[1];
    StreamMessage[] messages = new StreamMessage[1];
    serverStream.listen().subscribe(event -> {
      subjects[0] = event.getMessage().get().subject();
      messages[0] = event.getMessage().get();
    });

    channelContext.postReadSuccess(StreamMessage.builder().qualifier("q").build());
    assertEquals(id, subjects[0]);
    assertEquals("q", messages[0].qualifier());
    assertEquals(id, messages[0].subject());
  }

  @Test
  public void testServerStreamListenMessageHasAlreadyIdentity() throws Exception {
    String id = channelContext.getId();

    String[] subjects = new String[1];
    StreamMessage[] messages = new StreamMessage[1];
    serverStream.listen().subscribe(event -> {
      subjects[0] = event.getMessage().get().subject();
      messages[0] = event.getMessage().get();
    });

    String expectedSubject = "aaa/bbb" + "/" + id;
    channelContext.postReadSuccess(StreamMessage.builder().qualifier("q").subject("aaa/bbb").build());
    assertEquals(expectedSubject, subjects[0]);
    assertEquals("q", messages[0].qualifier());
    assertEquals(expectedSubject, messages[0].subject());
  }

  @Test
  public void testServerStreamSendMessageWithNoIdentity() throws Exception {
    PublishSubject<Object> subject = PublishSubject.create();
    channelContext.listen().subscribe(subject);
    serverStream.send(StreamMessage.builder().qualifier("q").build());
    subject.onCompleted();
    assertTrue(subject.isEmpty().toBlocking().toFuture().get());
  }

  @Test
  public void testServerStreamSendMessageWithShortIdentity() throws Exception {
    String id = channelContext.getId();

    Event.Topic[] topics = new Event.Topic[1];
    String[] msgIdentities = new String[1];
    channelContext.listen().subscribe(event -> {
      topics[0] = event.getTopic();
      msgIdentities[0] = event.getMessage().get().subject();
    });

    serverStream.send(StreamMessage.builder().qualifier("q").subject(id).build());

    assertEquals(Event.Topic.Write, topics[0]);
    assertEquals(null, msgIdentities[0]);
  }

  @Test
  public void testServerStreamSendMessageWithLongIdentity() throws Exception {
    String id = channelContext.getId();

    Event.Topic[] topics = new Event.Topic[1];
    String[] msgIdentities = new String[1];
    channelContext.listen().subscribe(event -> {
      topics[0] = event.getTopic();
      msgIdentities[0] = event.getMessage().get().subject();
    });

    serverStream.send(StreamMessage.builder().qualifier("q").subject("aaa/bbb" + "/" + id).build());

    assertEquals(Event.Topic.Write, topics[0]);
    assertEquals("aaa/bbb", msgIdentities[0]);
  }
}
