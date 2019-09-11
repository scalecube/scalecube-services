package io.scalecube.services.routings.sut;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.Tag;
import reactor.core.publisher.Flux;

@Service
@Tag(key = "tagA", value = "a")
@Tag(key = "tagB", value = "b")
@FunctionalInterface
public interface TagService {

  @ServiceMethod
  @Tag(key = "methodTagA", value = "a")
  Flux<String> upperCase(Flux<String> input);
}
