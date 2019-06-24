package io.scalecube.services.routings.sut;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Flux;

@Service(tags = {"tagA", "a", "tagB", "b"})
@FunctionalInterface
public interface TagService {

  @ServiceMethod(tags = {"methodTagA", "a"})
  Flux<String> upperCase(Flux<String> input);
}
