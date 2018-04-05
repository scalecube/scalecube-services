package io.scalecube.services;

import io.scalecube.streams.ClientStream;
import io.scalecube.streams.Event;
import io.scalecube.streams.ListeningServerStream;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.streams.codec.StreamMessageDataCodecImpl;
import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;

import java.util.concurrent.TimeUnit;

public class StreamDataCodecTest {

  public static final String QUALIFIER = "qual";
  public static final String PING = "ping";
  public static final String DOUBLE_PING = "pingping";
  public static final int TIMEOUT = 3;

  private StreamMessageDataCodec codec = new StreamMessageDataCodecImpl();

  private ListeningServerStream listeningServerStream;
  private ClientStream client;


  @Before
  public void setUp() {
    listeningServerStream = ListeningServerStream.newListeningServerStream();
    client = ClientStream.newClientStream();
  }

  @After
  public void tearDown() {
    client.close();
    listeningServerStream.close();
  }

  @Test
  public void testDataSerializationInStreams() {
    // Given:
    listeningServerStream.listenReadSuccess().map(Event::getMessageOrThrow).subscribe(req -> {
      try {
        StreamMessage deserialized = codec.decodeData(req, String.class);
        String responsePayload = (String) deserialized.data() + deserialized.data();
        StreamMessage toConsumer = codec.encodeData(StreamMessage.from(req).data(responsePayload).build());
        listeningServerStream.send(toConsumer);
      } catch (Throwable e) {
        System.out.println("Err occurred " + e.getMessage());
        Assert.fail();
      }
    });
    Address address = listeningServerStream.bindAwait();
    AssertableSubscriber<String> verify =
        client.listenReadSuccess().map(e -> (String) codec.decodeData(e.getMessageOrThrow(), String.class).data())
            .test();


    // When:
    StreamMessage req = codec.encodeData(StreamMessage.builder().qualifier(QUALIFIER).data(PING).build());
    client.send(address, req);

    // Then:
    String response = verify.awaitValueCount(1, TIMEOUT, TimeUnit.SECONDS)
        .assertValueCount(1)
        .getOnNextEvents().get(0);
    Assert.assertEquals(DOUBLE_PING, response);
  }
}
