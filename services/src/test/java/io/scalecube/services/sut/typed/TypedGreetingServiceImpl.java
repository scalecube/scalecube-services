package io.scalecube.services.sut.typed;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TypedGreetingServiceImpl implements TypedGreetingService {

  @Override
  public Flux<Shape> helloPolymorph() {
    return Flux.just(new Circle(1.0), new Rectangle(1.0, 2.0), new Square(5.0));
  }

  @Override
  public Flux<Object> helloMultitype(String t) {
    if ("shape".equals(t)) {
      return Flux.just(new Circle(1.0), new Rectangle(1.0, 2.0), new Square(5.0));
    }
    if ("trade_event".equals(t)) {
      return Flux.just(new StartOfDayEvent(), new EndOfDayEvent(), new TradeExecutedEvent());
    }
    throw new IllegalArgumentException("Unsupported type: " + t);
  }

  @Override
  public Flux<?> helloWildcardMultitype(String t) {
    return Flux.just(
        // shapes
        new Circle(1.0),
        new Rectangle(1.0, 2.0),
        new Square(5.0),
        // events
        new StartOfDayEvent(),
        new EndOfDayEvent(),
        new TradeExecutedEvent());
  }

  @Override
  public Mono<Object[]> helloMultitypeArray(String t) {
    return null;
  }
}
