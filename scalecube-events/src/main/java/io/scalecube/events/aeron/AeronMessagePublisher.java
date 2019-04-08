package io.scalecube.events.aeron;

import io.scalecube.events.api.MessagePublisher;
import reactor.core.publisher.TopicProcessor;

public class AeronMessagePublisher implements MessagePublisher {

  private final TopicProcessor<String> processor;

  public AeronMessagePublisher(TopicProcessor<String> processor) {
    this.processor = processor;
  }

  @Override
  public void send(String next) {
    processor.onNext(next);
  }
}
