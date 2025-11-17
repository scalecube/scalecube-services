package io.scalecube.services;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public final class MaskUtil {

  private MaskUtil() {
    // Do not instantiate
  }

  /**
   * Mask sensitive data by replacing part of string with an asterisk symbol.
   *
   * @param data sensitive data to be masked
   * @return masked data
   */
  public static String mask(String data) {
    if (data == null || data.length() < 5) {
      return "*****";
    }
    return data.replace(data.substring(2, data.length() - 2), "***");
  }

  /**
   * Mask sensitive data by replacing part of string with an asterisk symbol.
   *
   * @param data sensitive data to be masked
   * @return masked data
   */
  public static String mask(UUID data) {
    return data != null ? mask(data.toString()) : null;
  }

  /**
   * Mask sensitive data by replacing part of string with an asterisk symbol.
   *
   * @param map map with sensitive data to be masked
   * @return string representation
   */
  public static String mask(Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return String.valueOf(map);
    }
    return map.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> mask(entry.getValue())))
        .toString();
  }

  /**
   * Mask sensitive data by replacing part of string with an asterisk symbol.
   *
   * @param map map sensitive data to be masked
   * @param sensitiveKeys keys whose corresponding values should be masked
   * @return string representation
   */
  public static String mask(Map<?, ?> map, Set<String> sensitiveKeys) {
    if (map == null || map.isEmpty()) {
      return String.valueOf(map);
    }

    return map.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> String.valueOf(entry.getKey()),
                entry -> {
                  final var key = String.valueOf(entry.getKey());
                  final var value = String.valueOf(entry.getValue());
                  if (entry.getKey() == null || entry.getValue() == null) {
                    return value;
                  }
                  return sensitiveKeys.contains(key) ? MaskUtil.mask(value) : value;
                },
                (v1, v2) -> v1,
                LinkedHashMap::new))
        .toString();
  }
}
