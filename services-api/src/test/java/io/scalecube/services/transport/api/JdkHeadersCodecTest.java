package io.scalecube.services.transport.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class JdkHeadersCodecTest {

  private final HeadersCodec codec = new JdkCodec();

  @ParameterizedTest
  @MethodSource("provider")
  void test(Map<String, String> headers) throws IOException {
    Map<String, String> decoded = writeAndRead(headers);
    assertEquals(headers, decoded);
  }

  static Stream<Map<String, String>> provider() {
    Map<String, String> sampleMap = new HashMap<>();
    sampleMap.put("header", "value");
    sampleMap.put("test", "3");

    return Stream.of(
        sampleMap,
        Collections.singletonMap("header", String.valueOf(Integer.MAX_VALUE)),
        Collections.emptyMap(),
        Collections.singletonMap("", ""),
        Collections.singletonMap("header", ""),
        Collections.singletonMap("", "value"));
  }

  private Map<String, String> writeAndRead(Map<String, String> headers) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      codec.encode(os, headers);
      byte[] bytes = os.toByteArray();
      try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
        return codec.decode(is);
      }
    }
  }
}
