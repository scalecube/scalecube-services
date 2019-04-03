package io.scalecube.events.aeron;

import io.scalecube.events.api.Destination;
import io.scalecube.events.api.MessagePublisher;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;

public class AeronMessagePublisher implements MessagePublisher {

  private final AeronResources resources;

  private final TopicProcessor<String> processor = TopicProcessor.create();

  public AeronMessagePublisher(Destination dest) {
    this.resources = new AeronResources().useTmpDir().start().block();
    start(dest).block();
  }

  private Mono<Void> start(Destination dest) {
    return AeronServer.create(resources)
        .options(dest.host(), dest.port(), dest.controlPort())
        .handle(
            connection -> {
              System.out.println("AeronMessagePublisher connected to: " + dest);
              return connection.outbound().sendString(processor.log("send"));
            })
        .bind()
        .then();
  }

  @Override
  public void send(String next) {
    processor.onNext(next);
  }
}
