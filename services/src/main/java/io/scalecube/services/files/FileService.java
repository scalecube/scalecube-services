package io.scalecube.services.files;

import reactor.core.publisher.Mono;

/**
 * Local API for adding files of this microservices instance. Added files will be accessible by
 * {@link FileStreamer}. It is not a service: it is not announced in discovery and cannot be called
 * remotely, since it takes a local {@link java.io.File}. Typical usage: client registers {@link
 * LocalFileService} (it serves files as {@link FileStreamer}) and passes the same instance to app
 * services; an app service generates a file, calls {@link #addFile(AddFileRequest)}, then returns
 * result (file path qualifier) all the way back to the caller. On the caller side file path
 * qualifier gets combined with http-gateway address, and then url for file download is ready.
 */
public interface FileService {

  /**
   * Adds a file and returning path qualifier for the added file. {@link AddFileRequest} must
   * contain {@code file} that exists, that is not directory, and must have valid path, another
   * parameter - {@code ttl} (optional) represents time after which file will be deleted. Returned
   * file path qualifier comes as: {@code v1/endpoints/${microservices:id}/files/:name}, for
   * example: {@code v1/endpoints/19e3afc1-fa46-4a55-8a41-1bdf4afc8a5b/files/report_03_01_2025.txt}.
   * Fails with {@link IllegalStateException} until the instance is registered on a started
   * microservices instance.
   *
   * @param request request
   * @return async result with path qualifier
   */
  Mono<String> addFile(AddFileRequest request);
}
