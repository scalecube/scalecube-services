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

  public Integer getInt(String name) {
    return get(name, Integer::parseInt);
  }

  public Long getLong(String name) {
    return get(name, Long::parseLong);
  }

  public BigInteger getBigInteger(String name) {
    return get(name, BigInteger::new);
  }

  public Double getDouble(String name) {
    return get(name, Double::parseDouble);
  }

  public BigDecimal getBigDecimal(String name) {
    return get(name, BigDecimal::new);
  }

  public <T extends Enum<T>> T getEnum(String name, Function<String, T> enumFunc) {
    return get(name, enumFunc);
  }

  public Boolean getBoolean(String name) {
    return get(name, Boolean::parseBoolean);
  }

  public String getString(String name) {
    return get(name, s -> s);
  }

  public <T> T get(String name, Function<String, T> converter) {
    final var s = params.get(name);
    return s != null ? converter.apply(s) : null;
  }
}
