package io.scalecube.services.codec;

import io.scalecube.services.ServiceLoaderUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public interface DataCodec {

  Map<String, DataCodec> INSTANCES = new ConcurrentHashMap<>();

  static DataCodec getInstance(String contentType) {
    return INSTANCES.computeIfAbsent(contentType, DataCodec::loadInstance);
  }

  static DataCodec loadInstance(String contentType) {
    Optional<DataCodec> result = ServiceLoaderUtil.findFirst(DataCodec.class,
        codec -> codec.contentType().equalsIgnoreCase(contentType));
    return result.orElseThrow(() -> new IllegalStateException("DataCodec not configured"));
  }

  String contentType();

  void encode(OutputStream stream, Object value) throws IOException;

  Object decode(InputStream stream, Class<?> type) throws IOException;

}
