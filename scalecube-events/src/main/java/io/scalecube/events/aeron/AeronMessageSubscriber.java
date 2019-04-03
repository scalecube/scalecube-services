package io.scalecube.events.aeron;

import io.scalecube.events.api.Destination;
import io.scalecube.events.api.MessageSubscriber;
import io.scalecube.events.api.Topic;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronConnection;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;

public class AeronMessageSubscriber implements MessageSubscriber {

  private final AeronResources resources;

  private final TopicProcessor<Destination> addTopic = TopicProcessor.create();

  private final TopicProcessor<String> inboundProcessor = TopicProcessor.create();

  public AeronMessageSubscriber(Topic topic) {
    this.resources = new AeronResources().useTmpDir().start().block();

    addTopic.subscribe(
        dest -> {
          connect(dest)
              .subscribe(
                  cnn -> {
                    System.out.println("AeronMessageSubscriber connected to: " + dest);
                    cnn.inbound()
                        .receive()
                        .asString()
                        .doOnNext(
                            nxt -> {
                              inboundProcessor.onNext(nxt);
                            })
                        .subscribe();
                  });
        });
  }

  public void add(Destination destination) {
    addTopic.onNext(destination);
  }

  public FluxSink<Destination> addSink() {
    return addTopic.sink();
  }

  @Override
  public Flux<String> listen() {
    return inboundProcessor;
  }

  private Mono<? extends AeronConnection> connect(Destination address) {
    return AeronClient.create(resources)
        .options(address.host(), address.port(), address.controlPort())
        .connect();
  }
}
