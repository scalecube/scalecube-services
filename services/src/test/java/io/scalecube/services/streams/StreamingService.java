package io.scalecube.services.streams;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import rx.Observable;

@Service
public interface StreamingService {

  @ServiceMethod
  Observable<String> quotes(int index);

}
