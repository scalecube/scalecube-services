package io.scalecube.services.gateway.http;

import io.scalecube.services.gateway.sut.typed.Circle;
import io.scalecube.services.gateway.sut.typed.Rectangle;
import io.scalecube.services.gateway.sut.typed.Shape;
import io.scalecube.services.gateway.sut.typed.Square;
import io.scalecube.services.gateway.sut.typed.StartOfDayEvent;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import reactor.core.publisher.Mono;

public class TypedGreetingServiceImpl implements TypedGreetingService {

  @Override
  public Mono<Shape> helloPolymorph() {
    return Mono.just(new Circle(1.0));
  }

  @Override
  public Mono<List<Shape>> helloListPolymorph() {
    return Mono.just(List.of(new Circle(1.0), new Rectangle(1.0, 1.0), new Square(1.0)));
  }

  @Override
  public Mono<Object> helloMultitype() {
    return Mono.just(new StartOfDayEvent(1, 1, 1, LocalDateTime.now(Clock.systemUTC())));
  }

  @Override
  public Mono<?> helloWildcardMultitype() {
    return Mono.just(new StartOfDayEvent(1, 1, 1, LocalDateTime.now(Clock.systemUTC())));
  }
}
