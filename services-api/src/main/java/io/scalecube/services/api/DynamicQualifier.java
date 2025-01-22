package io.scalecube.services.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/**
 * Representation of dynamic qualifier. Being used in service method definitions along with normal
 * qualifiers. Typical example of dynamic qualifiers:
 *
 * <ul>
 *   <li>v1/api/users/:userId
 *   <li>v1/api/orders/:orderId/
 *   <li>v1/api/categories/:categoryId/products/:productId
 * </ul>
 */
public final class DynamicQualifier {

  private static final Pattern DYNAMIC_QUALIFIER_PATTERN = Pattern.compile("(^|/):\\w+(?:/|$)");

  private final String qualifier;
  private final Pattern pattern;
  private final List<String> pathVariables;
  private final int size;

  private DynamicQualifier(String qualifier) {
    final var list = new ArrayList<String>();
    final var builder = new StringBuilder();

    for (var s : qualifier.split("/")) {
      if (s.startsWith(":")) {
        final var pathVar = s.substring(1);
        builder.append("(?<").append(pathVar).append(">.+)");
        list.add(pathVar);
      } else {
        builder.append(s);
      }
      builder.append("/");
    }
    builder.setLength(builder.length() - 1);

    this.qualifier = qualifier;
    this.pattern = Pattern.compile(builder.toString());
    this.pathVariables = Collections.unmodifiableList(list);
    this.size = sizeOf(qualifier);
  }

  /**
   * Creates new {@link DynamicQualifier} instance.
   *
   * @param qualifier qualifier
   * @return {@link DynamicQualifier} instance
   */
  public static DynamicQualifier from(String qualifier) {
    return new DynamicQualifier(qualifier);
  }

  /**
   * Returns whether given qualifier is dynamic qualifier or not.
   *
   * @param qualifier qualifier
   * @return result
   */
  public static boolean isDynamicQualifier(String qualifier) {
    return DYNAMIC_QUALIFIER_PATTERN.matcher(qualifier).find();
  }

  /**
   * Original qualifier.
   *
   * @return result
   */
  public String qualifier() {
    return qualifier;
  }

  /**
   * Compiled pattern.
   *
   * @return result
   */
  public Pattern pattern() {
    return pattern;
  }

  /**
   * Returns path variable names.
   *
   * @return path variable names
   */
  public List<String> pathVariables() {
    return pathVariables;
  }

  /**
   * Size of qualifier. This is a number of {@code /} symbols.
   *
   * @return result
   */
  public int size() {
    return size;
  }

  /**
   * Matches input qualifier against this dynamic qualifier.
   *
   * @param qualifier qualifier
   * @return matched path variables key-value map, or null if no matching occurred
   */
  public Map<String, String> matchQualifier(String qualifier) {
    if (size != sizeOf(qualifier)) {
      return null;
    }

    final var matcher = pattern.matcher(qualifier);
    if (!matcher.matches()) {
      return null;
    }

    final var map = new LinkedHashMap<String, String>();
    for (var pathVar : pathVariables) {
      final var value = matcher.group(pathVar);
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException("Wrong path variable: " + pathVar);
      }
      map.put(pathVar, value);
    }

    return map;
  }

  private static int sizeOf(String value) {
    int count = 0;
    for (int i = 0, length = value.length(); i < length; i++) {
      if (value.charAt(i) == '/') {
        count++;
      }
    }
    return count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return Objects.equals(qualifier, ((DynamicQualifier) o).qualifier);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(qualifier);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", DynamicQualifier.class.getSimpleName() + "[", "]")
        .add("qualifier='" + qualifier + "'")
        .add("pattern=" + pattern)
        .add("pathVariables=" + pathVariables)
        .add("size=" + size)
        .toString();
  }
}
