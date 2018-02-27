package io.scalecube.ipc;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import io.scalecube.transport.Address;

import org.junit.Test;

public class ListeningServerStreamTest {

  @Test
  public void testServerStreamBindsManyTimes() throws Exception {
    ListeningServerStream serverStream0 = ListeningServerStream.newServerStream().withListenAddress("localhost");
    ListeningServerStream serverStream1 = serverStream0.bind();
    ListeningServerStream serverStream2 = serverStream0.bind();
    ListeningServerStream serverStream3 = serverStream0.bind();
    try {
      assertThat(serverStream0, not(sameInstance(serverStream1)));
      assertThat(serverStream1, not(sameInstance(serverStream2)));
      assertThat(serverStream2, not(sameInstance(serverStream3)));
    } finally {
      serverStream0.close();
      serverStream1.close();
      serverStream2.close();
      serverStream3.close();
    }
  }

  @Test
  public void testServerStreamBindsOnAvailablePort() throws Exception {
    Address address1 = Address.create("127.0.0.1", 5801);
    Address address2 = Address.create("127.0.0.1", 5802);
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().withListenAddress("localhost");
    ListeningServerStream serverStream1 = serverStream.bind();
    ListeningServerStream serverStream2 = serverStream.bind();
    try {
      assertEquals(address1, serverStream1.listenBind().toBlocking().toFuture().get());
      assertEquals(address2, serverStream2.listenBind().toBlocking().toFuture().get());
    } finally {
      serverStream1.close();
      serverStream2.close();
    }
  }

  @Test
  public void testServerStreamBindsThenUnbinds() throws Exception {
    Address address = Address.create("127.0.0.1", 5801);
    ListeningServerStream temaplte = ListeningServerStream.newServerStream().withListenAddress("localhost");
    ListeningServerStream stream1 = temaplte.bind();
    try {
      assertEquals(address, stream1.listenBind().toBlocking().toFuture().get());
    } finally {
      stream1.close();
    }
    // After previous successfull (hopefully) close() it's possible to bind again on port
    ListeningServerStream stream2 = temaplte.bind();
    try {
      assertEquals(address, stream2.listenBind().toBlocking().toFuture().get());
    } finally {
      stream2.close();
    }
  }

  @Test
  public void testServerStreamListenBindUnbindAfterClose() throws Exception {
    Address address = Address.create("127.0.0.1", 5801);
    ListeningServerStream temaplte = ListeningServerStream.newServerStream().withListenAddress("localhost");

    ListeningServerStream serverStream = temaplte.bind();
    // check we received address on which we were bound
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());
    // .. and again
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());

    serverStream.close();
    // check we received address on which we were unbound
    assertEquals(address, temaplte.listenUnbind().toBlocking().toFuture().get());
    // .. and again
    assertEquals(address, temaplte.listenUnbind().toBlocking().toFuture().get());
    // plus we can listen to bind even after server stream close
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());
  }
}
