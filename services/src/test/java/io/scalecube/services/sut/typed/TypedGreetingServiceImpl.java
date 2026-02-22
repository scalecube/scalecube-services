package io.scalecube.services.sut.typed;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import reactor.core.publisher.Flux;

public class TypedGreetingServiceImpl implements TypedGreetingService {

  @Override
  public Flux<Shape> helloPolymorph() {
    return Flux.just(new Circle(1.0), new Rectangle(1.0, 1.0), new Square(1.0));
  }

  @Override
  public Flux<Object> helloMultitype(String t) {
    if ("shape".equals(t)) {
      return Flux.just(new Circle(1.0), new Rectangle(1.0, 1.0), new Square(1.0));
    }
    if ("trade_event".equals(t)) {
      return Flux.just(
          new StartOfDayEvent(1, 1, 1, LocalDateTime.now(Clock.systemUTC())),
          new EndOfDayEvent(1, 2, 2, LocalDateTime.now(Clock.systemUTC())),
          new TradeExecutedEvent(1, 3, 3, new BigDecimal("100"), new BigDecimal("100"), 100));
    }
    throw new IllegalArgumentException("Unsupported type: " + t);
  }

  @Override
  public Flux<?> helloWildcardMultitype(String t) {
    return Flux.just(
        // shapes
        new Circle(1.0),
        new Rectangle(1.0, 1.0),
        new Square(1.0),
        // events
        new StartOfDayEvent(1, 1, 1, LocalDateTime.now(Clock.systemUTC())),
        new EndOfDayEvent(1, 2, 2, LocalDateTime.now(Clock.systemUTC())),
        new TradeExecutedEvent(1, 3, 3, new BigDecimal("100"), new BigDecimal("100"), 100500));
  }
}
