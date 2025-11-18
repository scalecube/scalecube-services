package io.scalecube.services;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class TypedParameters {

  private final Map<String, String> params;

  public TypedParameters(Map<String, String> params) {
    this.params = new LinkedHashMap<>(params != null ? params : Map.of());
  }

  public Integer intValue(String name) {
    return get(name, Integer::parseInt);
  }

  public Long longValue(String name) {
    return get(name, Long::parseLong);
  }

  public BigInteger bigInteger(String name) {
    return get(name, BigInteger::new);
  }

  public Double doubleValue(String name) {
    return get(name, Double::parseDouble);
  }

  public BigDecimal bigDecimal(String name) {
    return get(name, BigDecimal::new);
  }

  public <T extends Enum<T>> T enumValue(String name, Function<String, T> enumFunc) {
    return get(name, enumFunc);
  }

  public Boolean booleanValue(String name) {
    return get(name, Boolean::parseBoolean);
  }

  public String stringValue(String name) {
    return get(name, s -> s);
  }

  public <T> T get(String name, Function<String, T> converter) {
    final var s = params.get(name);
    return s != null ? converter.apply(s) : null;
  }
}
