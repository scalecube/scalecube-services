package io.scalecube.services.codec;

import io.scalecube.services.ServiceLoaderUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface DataCodec {

  Map<String, DataCodec> INSTANCES = new ConcurrentHashMap<>();

  static DataCodec getInstance(String contentType) {
    return INSTANCES.computeIfAbsent(contentType, DataCodec::loadInstance);
  }

  /**
   * Get a DataCodec for a content type.
   * 
   * @param contentType the content type.
   * @return a Data codec for the content type or IllegalArgumentException is thrown if non exist
   */
  static DataCodec loadInstance(String contentType) {
    return ServiceLoaderUtil.findFirst(DataCodec.class,
        codec -> codec.contentType().equalsIgnoreCase(contentType))
        .orElseThrow(() -> new IllegalArgumentException("DataCodec not configured"));
  }

  String contentType();

  void encode(OutputStream stream, Object value) throws IOException;

  Object decode(InputStream stream, Class<?> type) throws IOException;

}
