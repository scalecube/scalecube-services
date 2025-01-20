package io.scalecube.services.files;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service
public interface FileService {

  @ServiceMethod
  Mono<String> addFile(AddFileRequest request);
}
