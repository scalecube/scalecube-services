package io.scalecube.services.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Pattern;

public final class DynamicQualifier {

  private final String qualifier;
  private final Pattern pattern;
  private final List<String> pathVariables = new ArrayList<>();

  public DynamicQualifier(String qualifier) {
    if (!qualifier.contains(":")) {
      throw new IllegalArgumentException("Illegal dynamic qualifier: " + qualifier);
    }
    final StringBuilder sb = new StringBuilder();
    for (var s : qualifier.split("/")) {
      if (s.startsWith(":")) {
        final var pathVar = s.substring(1);
        sb.append("(?<").append(pathVar).append(">.*?)");
        pathVariables.add(pathVar);
      } else {
        sb.append(s);
      }
      sb.append("/");
    }
    sb.setLength(sb.length() - 1);
    this.pattern = Pattern.compile(sb.toString());
    this.qualifier = qualifier;
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

  public Map<String, String> matchQualifier(String input) {
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
        .toString();
  }
}
