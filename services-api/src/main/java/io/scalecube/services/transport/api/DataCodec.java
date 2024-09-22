package io.scalecube.services.transport.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface DataCodec {

  Map<String, DataCodec> INSTANCES =
      StreamSupport.stream(ServiceLoader.load(DataCodec.class).spliterator(), false)
          .collect(Collectors.toMap(DataCodec::contentType, Function.identity()));

  static Collection<DataCodec> getAllInstances() {
    return INSTANCES.values();
  }

  static Set<String> getAllContentTypes() {
    return getAllInstances().stream().map(DataCodec::contentType).collect(Collectors.toSet());
  }

  /**
   * Returns {@link DataCodec} by given {@code contentType}.
   *
   * @param contentType contentType (required)
   * @return {@link DataCodec} by given {@code contentType} (or throws IllegalArgumentException is
   *     thrown if not exist)
   */
  static DataCodec getInstance(String contentType) {
    Objects.requireNonNull(contentType, "[getInstance] contentType");
    DataCodec dataCodec = INSTANCES.get(contentType);
    Objects.requireNonNull(
        dataCodec, "[getInstance] dataCodec not found for '" + contentType + "'");
    return dataCodec;
  }

  String contentType();

  void encode(OutputStream stream, Object value) throws IOException;

  Object decode(InputStream stream, Type type) throws IOException;
}
