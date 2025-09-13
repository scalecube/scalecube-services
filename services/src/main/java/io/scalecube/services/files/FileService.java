package io.scalecube.services.files;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

/**
 * Service interface for adding files locally. Added files will be accessible by {@link
 * FileStreamer}. Typical usage: client defines an app service with injected {@link FileService},
 * client generates a file in the app service, then calls {@link #addFile(AddFileRequest)}, then
 * returns result (file path qualifier) all the way back to the caller of app service. On the caller
 * side file path qualifier gets combined with http-gateway address, and then url for file download
 * is ready.
 */
@Service
public interface FileService {

  /**
   * Adds a file and returning path qualifier for the added file. {@link AddFileRequest} must
   * contain {@code file} that exists, that is not directory, and must have valid path, another
   * parameter - {@code ttl} (optional) represents time after which file will be deleted. Returned
   * file path qualifier comes as: {@code v1/scalecube.endpoints/${microservices:id}/files/:name},
   * for example: {@code
   * v1/scalecube.endpoints/19e3afc1-fa46-4a55-8a41-1bdf4afc8a5b/files/report_03_01_2025.txt}
   *
   * @param request request
   * @return async result with path qualifier
   */
  @ServiceMethod
  Mono<String> addFile(AddFileRequest request);
}
