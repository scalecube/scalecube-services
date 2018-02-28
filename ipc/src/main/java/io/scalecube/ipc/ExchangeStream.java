package io.scalecube.ipc;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Address;

import rx.Observable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ExchangeStream implements EventStream {
  private static final Address ADDRESS = Address.from("ExchangeStream:0");

  // private final String streamId = IdGenerator.generateId();

  private static final ServiceMessage onCompletedMessage =
      ServiceMessage.withQualifier("io.scalecube.ipc/onCompleted")/* .streamId(streamId) */.build();

  private static final ConcurrentMap<String, ExchangeStream> idToExchangeStream = new ConcurrentHashMap<>();

  // private final Address address;
  // private final ClientStream clientStream;
  private final ChannelContext channelContext = ChannelContext.create(IdGenerator.generateId(), ADDRESS);
  private final ServerStream serverStream = ServerStream.newServerStream();

  public ExchangeStream(Address address, ClientStream clientStream) {
    // this.address = address;
    // this.clientStream = clientStream;
    serverStream.subscribe(channelContext);

    serverStream.listen()
        .filter(Event::isMessageWrite)
        .map(event -> event.getMessage().get())
        .subscribe(message -> clientStream.send(address, message));

    // clientStream.listen();
    // serverStream.send();

    idToExchangeStream.put(channelContext.getId(), this);
  }

  public void onNext(ServiceMessage message) {
    // clientStream.send(address, ServiceMessage.copyFrom(message).streamId(streamId).build());
    channelContext.postMessageWrite(message);
  }

  public void onCompleted() {
    channelContext.postMessageWrite(onCompletedMessage);
  }

  @Override
  public void subscribe(ChannelContext channelContext) {
    // no-op
  }

  @Override
  public Observable<Event> listen() {
    return channelContext;
  }

  @Override
  public void close() {
    channelContext.close();
    serverStream.close();
  }

  public static void main(String[] args) {
    ClientStream clientStream0 = ClientStream.newClientStream();
    ClientStream clientStream1 = ClientStream.newClientStream();

    ExchangeStream stream0 = new ExchangeStream(Address.from("127.0.0.1:5801"), clientStream0);
    stream0.onNext(ServiceMessage.withQualifier("a").build());
    stream0.onNext(ServiceMessage.withQualifier("b").build());
    stream0.onNext(ServiceMessage.withQualifier("c").build());
    stream0.onCompleted();

    ExchangeStream stream1 = new ExchangeStream(Address.from("127.0.0.1:5802"), clientStream1);
    stream1.onNext(ServiceMessage.withQualifier("x").build());
    stream1.onNext(ServiceMessage.withQualifier("y").build());
    stream1.onNext(ServiceMessage.withQualifier("z").build());
    stream1.onCompleted();


  }
}
