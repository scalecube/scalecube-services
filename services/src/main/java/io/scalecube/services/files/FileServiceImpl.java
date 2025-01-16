package io.scalecube.services.files;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.AfterConstruct;
import java.io.File;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FileServiceImpl implements FileService, FileStreamer {

  private String serviceEndpointId;

  @AfterConstruct
  void conclude(Microservices microservices) {
    serviceEndpointId = microservices.serviceEndpoint().id();
  }

  @Override
  public Mono<String> addFile(File file, Duration duration) {
    // TODO: v1/scalecube.endpoints/$serviceEndpointId/files/$filename
    return null;
  }

  @Override
  public Flux<byte[]> streamFile() {
    // TODO: implement something similar to ByteBufFlux
    return null;
  }
}
