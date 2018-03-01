package io.scalecube.examples.gateway;

import static io.scalecube.ipc.ServiceMessage.copyFrom;

import io.scalecube.ipc.Event;
import io.scalecube.ipc.TransportStream;
import io.scalecube.ipc.ListeningServerStream;
import io.scalecube.ipc.ServiceMessage;
import io.scalecube.transport.Address;

import rx.Observable;

import java.util.Optional;

public class ServiceZipEchoRunner {

  public static final Address ADDRESS1 = Address.from("127.0.1.1:5802");
  public static final Address ADDRESS2 = Address.from("127.0.1.1:5803");
  public static final Address ADDRESS3 = Address.from("127.0.1.1:5804");

  /**
   * Example for using transport streams.
   * 
   * @param args noop
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException {
    TransportStream transport = TransportStream.newTransportStream();

    ListeningServerStream serverStream = ListeningServerStream.newServerStream().withPort(5801).bind();
    serverStream.listenReadSuccess().subscribe(event -> {

      ServiceMessage message = event.getMessage().get();

      TransportStream transport1 = transport.send(ADDRESS1, copyFrom(message).qualifier("aaa").build());
      TransportStream transport2 = transport.send(ADDRESS2, copyFrom(message).qualifier("bbb").build());
      TransportStream transport3 = transport.send(ADDRESS3, copyFrom(message).qualifier("ccc").build());

      Observable<Event> listen1 = transport1.listen();
      Observable<Event> listen2 = transport2.listen();
      Observable<Event> listen3 = transport3.listen();

      Observable.zip(listen1, listen2, listen3,
          (event1, event2, event3) -> {
            Optional<ServiceMessage> message1 = event1.getMessage();
            Optional<ServiceMessage> message2 = event2.getMessage();
            Optional<ServiceMessage> message3 = event3.getMessage();

            System.out.println("ServiceZipEcho: "
                + "message1.senderId=" + message1.get().getSenderId()
                + ", message2.senderId=" + message2.get().getSenderId()
                + ", message3.senderId=" + message3.get().getSenderId());

            String qualifier1 = message1.get().getQualifier();
            String qualifier2 = message2.get().getQualifier();
            String qualifier3 = message3.get().getQualifier();

            String qualifier = qualifier1 + "/" + qualifier2 + "/" + qualifier3;

            return copyFrom(message).qualifier(qualifier).build();
          })
          .subscribe(serverStream::send);
    });

    Thread.currentThread().join();
  }
}
