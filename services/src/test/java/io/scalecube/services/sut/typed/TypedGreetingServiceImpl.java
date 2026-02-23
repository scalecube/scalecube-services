package io.scalecube.services.sut.typed;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TypedGreetingServiceImpl implements TypedGreetingService {

  @Override
  public Flux<Shape> helloPolymorph() {
    return Flux.just(new Circle(1.0), new Rectangle(1.0, 1.0), new Square(1.0));
  }

  @Override
  public Mono<List<Shape>> helloListPolymorph() {
    return Mono.just(List.of(new Circle(1.0), new Rectangle(1.0, 1.0), new Square(1.0)));
  }

  @Override
  public Flux<Object> helloMultitype() {
    return Flux.just(
        new StartOfDayEvent(1, 1, 1, LocalDateTime.now(Clock.systemUTC())),
        new EndOfDayEvent(1, 2, 2, LocalDateTime.now(Clock.systemUTC())),
        new TradeEvent(1, 3, 3, new BigDecimal("100"), new BigDecimal("100"), 100));
  }

  @Override
  public Flux<?> helloWildcardMultitype() {
    return Flux.just(
        new StartOfDayEvent(1, 1, 1, LocalDateTime.now(Clock.systemUTC())),
        new EndOfDayEvent(1, 2, 2, LocalDateTime.now(Clock.systemUTC())),
        new TradeEvent(1, 3, 3, new BigDecimal("100"), new BigDecimal("100"), 100));
  }
}
