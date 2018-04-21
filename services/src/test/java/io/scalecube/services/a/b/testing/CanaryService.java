package io.scalecube.services.a.b.testing;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import org.reactivestreams.Publisher;


@Service
public interface CanaryService {

  @ServiceMethod
  Publisher<String> greeting(String string);

}
