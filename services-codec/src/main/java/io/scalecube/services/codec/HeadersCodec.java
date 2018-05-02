package io.scalecube.services.codec;

import io.scalecube.services.ServiceLoaderUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public interface HeadersCodec {

  static HeadersCodec getInstance(String contentType) {
    Objects.requireNonNull(contentType);

    Optional<HeadersCodec> result = ServiceLoaderUtil.findFirst(HeadersCodec.class,
        codec -> codec.contentType().equalsIgnoreCase(contentType));

    return result.orElseThrow(() -> new IllegalStateException("HeadersCodec not configured"));
  }

  String contentType();

  void encode(OutputStream stream, Map<String, String> headers) throws IOException;

  Map<String, String> decode(InputStream stream) throws IOException;

}
