package io.scalecube.events.aeron;

import io.scalecube.events.api.Destination;
import io.scalecube.events.api.Topic;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.core.publisher.TopicProcessor;

public class AeronBroker {

  private final TopicProcessor<String> processor = TopicProcessor.create();

  AeronResources resources = new AeronResources().useTmpDir().start().block();

  AeronServer server;

  public AeronBroker server(Destination dest) {

    AeronResources resources = new AeronResources().useTmpDir().start().block();

    server = AeronServer.create(resources).options(dest.host(), dest.port(), dest.controlPort());

    server
        .handle(
            connection -> {
              return connection.outbound().sendString(processor.log("send"));
            })
        .bind()
        .then()
        .block();

    return this;
  }

  public AeronMessagePublisher publisher(Topic topic) {
    topic.processor().doOnNext(n -> processor.onNext(n));
    return new AeronMessagePublisher(this.processor);
  }

  public AeronMessageSubscriber subscriber(Topic topic) {
    return new AeronMessageSubscriber(topic);
  }
}
