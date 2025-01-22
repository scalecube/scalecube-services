package io.scalecube.services.files;

import java.io.File;
import java.time.Duration;
import java.util.StringJoiner;

public class AddFileRequest {

  private final File file;
  private final Duration ttl;

  public AddFileRequest(File file) {
    this(file, null);
  }

  public AddFileRequest(File file, Duration ttl) {
    this.file = file;
    this.ttl = ttl;
  }

  public File file() {
    return file;
  }

  public Duration ttl() {
    return ttl;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", AddFileRequest.class.getSimpleName() + "[", "]")
        .add("file=" + file)
        .add("ttl=" + ttl)
        .toString();
  }
}
