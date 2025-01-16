package io.scalecube.services.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Pattern;

public final class DynamicQualifier {

  private static final Pattern DYNAMIC_QUALIFIER_PATTERN = Pattern.compile("(^|/):\\w+(?:/|$)");

  private final String qualifier;
  private final Pattern pattern;
  private final List<String> pathVariables;
  private final int size;

  public DynamicQualifier(String qualifier) {
    final var pathVars = new ArrayList<String>();
    final var sb = new StringBuilder();
    for (var s : qualifier.split("/")) {
      if (s.startsWith(":")) {
        final var pathVar = s.substring(1);
        sb.append("(?<").append(pathVar).append(">.*?)");
        pathVars.add(pathVar);
      } else {
        sb.append(s);
      }
      sb.append("/");
    }
    sb.setLength(sb.length() - 1);

    this.qualifier = qualifier;
    this.pattern = Pattern.compile(sb.toString());
    this.pathVariables = Collections.unmodifiableList(pathVars);
    this.size = sizeOf(qualifier);
  }

  public static boolean isDynamicQualifier(String input) {
    return DYNAMIC_QUALIFIER_PATTERN.matcher(input).find();
  }

  public String qualifier() {
    return qualifier;
  }

  public Pattern pattern() {
    return pattern;
  }

  public List<String> pathVariables() {
    return pathVariables;
  }

  public int size() {
    return size;
  }

  public Map<String, String> matchQualifier(String input) {
    if (size != sizeOf(input)) {
      return null;
    }

    final var matcher = pattern.matcher(input);
    if (!matcher.matches()) {
      return null;
    }

    final var map = new LinkedHashMap<String, String>();
    for (var pathVar : pathVariables) {
      final var value = matcher.group(pathVar);
      Objects.requireNonNull(
          value, "Path variable value must not be null, path variable: " + pathVar);
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
