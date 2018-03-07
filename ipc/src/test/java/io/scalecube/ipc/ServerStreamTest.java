package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.membership.IdGenerator;
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
    channelContext = ChannelContext.create(IdGenerator.generateId(), Address.create("localhost", 0));
    serverStream = ServerStream.newServerStream();
    serverStream.subscribe(channelContext);
  }

  @Test
  public void testServerStreamListenMessageWithNoIdentity() throws Exception {
    String id = channelContext.getId();

    String[] identities = new String[1];
    ServiceMessage[] messages = new ServiceMessage[1];
    serverStream.listen().subscribe(event -> {
      identities[0] = event.getMessage().get().getSenderId();
      messages[0] = event.getMessage().get();
    });

    channelContext.postReadSuccess(ServiceMessage.withQualifier("q").build());
    assertEquals(id, identities[0]);
    assertEquals("q", messages[0].getQualifier());
    assertEquals(id, messages[0].getSenderId());
  }

  @Test
  public void testServerStreamListenMessageHasAlreadyIdentity() throws Exception {
    String id = channelContext.getId();

    String[] identities = new String[1];
    ServiceMessage[] messages = new ServiceMessage[1];
    serverStream.listen().subscribe(event -> {
      identities[0] = event.getMessage().get().getSenderId();
      messages[0] = event.getMessage().get();
    });

    String expectedSenderId = "aaa/bbb" + "/" + id;
    channelContext.postReadSuccess(ServiceMessage.withQualifier("q").senderId("aaa/bbb").build());
    assertEquals(expectedSenderId, identities[0]);
    assertEquals("q", messages[0].getQualifier());
    assertEquals(expectedSenderId, messages[0].getSenderId());
  }

  @Test
  public void testServerStreamSendMessageWithNoIdentity() throws Exception {
    PublishSubject<Object> subject = PublishSubject.create();
    channelContext.listen().subscribe(subject);
    serverStream.send(ServiceMessage.withQualifier("q").build());
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
      msgIdentities[0] = event.getMessage().get().getSenderId();
    });

    serverStream.send(ServiceMessage.withQualifier("q").senderId(id).build());

    assertEquals(Event.Topic.MessageWrite, topics[0]);
    assertEquals(null, msgIdentities[0]);
  }

  @Test
  public void testServerStreamSendMessageWithLongIdentity() throws Exception {
    String id = channelContext.getId();

    Event.Topic[] topics = new Event.Topic[1];
    String[] msgIdentities = new String[1];
    channelContext.listen().subscribe(event -> {
      topics[0] = event.getTopic();
      msgIdentities[0] = event.getMessage().get().getSenderId();
    });

    serverStream.send(ServiceMessage.withQualifier("q").senderId("aaa/bbb" + "/" + id).build());

    assertEquals(Event.Topic.MessageWrite, topics[0]);
    assertEquals("aaa/bbb", msgIdentities[0]);
  }
}
