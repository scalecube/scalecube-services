package io.scalecube.services.files;

import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.Tag;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;

/**
 * System service interface for streaming files after they have been added locally with {@link
 * FileService#addFile(AddFileRequest)}. NOTE: this is system service interface, clients are not
 * supposed to use it directly.
 */
@Service(FileStreamer.NAMESPACE)
public interface FileStreamer {

  String NAMESPACE = "v1/endpoints";

  @Tag(key = "Content-Type", value = "application/file")
  @RestMethod("GET")
  @ResponseType(byte[].class)
  @ServiceMethod("${microservices:id}/files/:filename")
  Flux<ServiceMessage> streamFile();
}
