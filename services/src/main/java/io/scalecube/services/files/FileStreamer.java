package io.scalecube.services.files;

import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.Tag;
import reactor.core.publisher.Flux;

@Service(FileStreamer.NAMESPACE)
public interface FileStreamer {

  String NAMESPACE = "v1/scalecube.endpoints";

  @Tag(key = "Content-Type", value = "application/scalecube-file")
  @RestMethod("GET")
  @ServiceMethod("${microservices:id}/files/:name")
  Flux<byte[]> streamFile();
}
