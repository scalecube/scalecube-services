package io.scalecube.services.files;

import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.Tag;
import reactor.core.publisher.Flux;

/**
 * System service interface for streaming files after they have been added locally with {@link
 * FileService#addFile(AddFileRequest)}. NOTE: this is system service inerface, clients are not
 * supposed to inject it into their app services and call it directly.
 */
@Service(FileStreamer.NAMESPACE)
public interface FileStreamer {

  String NAMESPACE = "v1/scalecube.endpoints";

  @Tag(key = "Content-Type", value = "application/scalecube-file")
  @RestMethod("GET")
  @ServiceMethod("${microservices:id}/files/:name")
  Flux<byte[]> streamFile();
}
