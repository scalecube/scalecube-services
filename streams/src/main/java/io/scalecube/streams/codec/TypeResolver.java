package io.scalecube.streams.codec;

import java.util.HashMap;
import java.util.Map;

public class TypeResolver {

  Map<String, Class> typeMap = new HashMap<>();

  public Class<?> resolveType(String qualifier) {
        return typeMap.get(qualifier);
    }

  public void registerType(String qualifier, Class clazz) {
    typeMap.put(qualifier, clazz);
  }

}
