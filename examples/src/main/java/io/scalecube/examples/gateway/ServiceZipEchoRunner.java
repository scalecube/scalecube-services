package io.scalecube.examples.gateway;

import static io.scalecube.ipc.ServiceMessage.copyFrom;

import io.scalecube.ipc.ExchangeStream;
import io.scalecube.ipc.ListeningServerStream;
import io.scalecube.ipc.ServiceMessage;
import io.scalecube.transport.Address;

import rx.Observable;

/**
 * Calls three requests and joins them together asynchronously.
 */
public class ServiceZipEchoRunner {

  public static final Address ADDRESS1 = Address.from("127.0.1.1:5802");
  public static final Address ADDRESS2 = Address.from("127.0.1.1:5803");
  public static final Address ADDRESS3 = Address.from("127.0.1.1:5804");

  /**
   * Main method.
   */
  public static void main(String[] args) throws InterruptedException {
    ExchangeStream exchangeStream = ExchangeStream.newExchangeStream();

    ListeningServerStream serverStream = ListeningServerStream.newServerStream().withPort(5801).bind();
    serverStream.listenMessageReadSuccess().subscribe(message -> {

      Observable<ServiceMessage> listen1 =
          exchangeStream.send(ADDRESS1, copyFrom(message).qualifier("aaa").build());
      Observable<ServiceMessage> listen2 =
          exchangeStream.send(ADDRESS2, copyFrom(message).qualifier("bbb").build());
      Observable<ServiceMessage> listen3 =
          exchangeStream.send(ADDRESS3, copyFrom(message).qualifier("ccc").build());

      Observable.zip(listen1, listen2, listen3,
          (message1, message2, message3) -> {
            String qualifier1 = message1.getQualifier();
            String qualifier2 = message2.getQualifier();
            String qualifier3 = message3.getQualifier();

            String qualifier = qualifier1 + "/" + qualifier2 + "/" + qualifier3;

            return copyFrom(message).qualifier(qualifier).build();
          })
          .subscribe(serverStream::send);
    });

    Thread.currentThread().join();
  }
}
