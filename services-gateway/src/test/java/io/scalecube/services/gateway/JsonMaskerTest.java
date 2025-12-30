package io.scalecube.services.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JsonMaskerTest {
  @ParameterizedTest
  @CsvSource({
    "'{\"username\":\"daria\", \"options\": \"ABC\"}', "
        + "'{\"username\":\"daria\", \"options\": \"ABC\"}'",
    "'nopassword\":\"LastPwd\"}', " + "'nopassword\":\"LastPwd\"}'",
    "'password\":\"LastPwd\"}', " + "'password\":\"*******\"}'",
    "'password\": LastPwd\", \"username\":\"daria\"}', "
        + "'password\": *******\", \"username\":\"daria\"}'",
    "'password\":\"LastPwd', " + "'password\":\"*******'",
    "'password\":LastPwd', " + "'password\":*******'",
    "'password\": 1234567, \"username\":\"daria\"}', "
        + "'password\": *******, \"username\":\"daria\"}'",
    "'{\"username\":\"daria\",\"password\":\"Test\\\"12345\", \"options\": \"ABC\"}', "
        + "'{\"username\":\"daria\",\"password\":\"***********\", \"options\": \"ABC\"}'",
    "'{\"username\":\"daria\",\"password\":\"Test\\\"12345\", \"options\": \"ABC\", \"password\":\"LastPwd\"}', "
        + "'{\"username\":\"daria\",\"password\":\"***********\", \"options\": \"ABC\", \"password\":\"*******\"}'"
  })
  void maskPassword(String input, String output) {
    System.out.println(input);
    System.out.println(output);
    final var bytes = input.getBytes(StandardCharsets.UTF_8);
    final var buffer = Unpooled.wrappedBuffer(bytes);
    JsonMasker.mask(buffer, 0, bytes.length, "password".getBytes(StandardCharsets.UTF_8), true);
    assertEquals(output, buffer.toString(StandardCharsets.UTF_8));
  }

  @ParameterizedTest
  @CsvSource({
    "'{\"name\":\"daria\", \"options\": \"ABC\"}', "
        + "'{\"name\":\"daria\", \"options\": \"ABC\"}'",
    "'nousername\":\"LastPwd\"}', " + "'nousername\":\"LastPwd\"}'",
    "'username\":\"1234\"}', " + "'username\":\"****\"}'",
    "'username\":\"12345\"}', " + "'username\":\"*****\"}'",
    "'username\":\"123456\"}', " + "'username\":\"12**56\"}'",
    "'username\":\"LastPwd\"}', " + "'username\":\"La***wd\"}'",
    "'username\": LastPwd\", \"name\":\"daria\"}', "
        + "'username\": La***wd\", \"name\":\"daria\"}'",
    "'username\":\"LastPwd', " + "'username\":\"La***wd'",
    "'username\":LastPwd', " + "'username\":La***wd'",
    "'username\": 1234567, \"name\":\"daria\"}', " + "'username\": 12***67, \"name\":\"daria\"}'",
    "'{\"name\":\"daria\",\"username\":\"Test\\\"12345\", \"options\": \"ABC\"}', "
        + "'{\"name\":\"daria\",\"username\":\"Te*******45\", \"options\": \"ABC\"}'",
    "'{\"name\":\"daria\",\"username\":\"Test\\\"12345\", \"options\": \"ABC\", \"username\":\"LastPwd\"}', "
        + "'{\"name\":\"daria\",\"username\":\"Te*******45\", \"options\": \"ABC\", \"username\":\"La***wd\"}'"
  })
  void maskUsername(String input, String output) {
    System.out.println(input);
    System.out.println(output);
    final var bytes = input.getBytes(StandardCharsets.UTF_8);
    final var buffer = Unpooled.wrappedBuffer(bytes);
    JsonMasker.mask(buffer, 0, bytes.length, "username".getBytes(StandardCharsets.UTF_8), false);
    assertEquals(output, buffer.toString(StandardCharsets.UTF_8));
  }
}
