package io.scalecube.services.files;

import java.io.File;
import java.time.Duration;
import java.util.StringJoiner;

public class AddFileRequest {

  private File file;
  private Duration ttl;

  public AddFileRequest() {}

  public AddFileRequest(File file, Duration ttl) {
    this.file = file;
    this.ttl = ttl;
  }

  public AddFileRequest(File file) {
    this.file = file;
  }

  public File file() {
    return file;
  }

  public AddFileRequest file(File file) {
    this.file = file;
    return this;
  }

  public Duration ttl() {
    return ttl;
  }

  public AddFileRequest ttl(Duration ttl) {
    this.ttl = ttl;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", AddFileRequest.class.getSimpleName() + "[", "]")
        .add("file=" + file)
        .add("ttl=" + ttl)
        .toString();
  }
}
