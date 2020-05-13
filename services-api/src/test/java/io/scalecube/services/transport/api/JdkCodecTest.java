package io.scalecube.services.transport.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class JdkCodecTest {

  private final DataCodec codec = new JdkCodec();

  @ParameterizedTest
  @MethodSource("provider")
  void test(Object body) throws IOException {
    Object decoded = writeAndRead(body);
    assertEquals(body, decoded);
  }

  static Stream<Object> provider() {
    return Stream.of("hello", Arrays.<Object>asList(1, 2, 3), new Greeting("joe"));
  }

  private Object writeAndRead(Object body) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      codec.encode(os, body);
      byte[] bytes = os.toByteArray();
      try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
        return codec.decode(is, body.getClass());
      }
    }
  }

  static class Greeting implements Serializable {

    private final String name;

    Greeting(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Greeting greeting = (Greeting) o;
      return Objects.equals(name, greeting.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

}
