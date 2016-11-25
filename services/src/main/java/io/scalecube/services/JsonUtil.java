package io.scalecube.services;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Create service Advertisement from a json format.
   * 
   * @param value json value of a registration info.
   * @return instance of service Advertisement.
   */
  public static <T> T from(String value, Class<T> clazz) {
    try {
      return (T) mapper.readValue(value, clazz);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Create json format of a service Advertisement.
   * 
   * @param value instance value of a Advertisement.
   * @return json value of a registration info.
   */
  public static String toJson(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
