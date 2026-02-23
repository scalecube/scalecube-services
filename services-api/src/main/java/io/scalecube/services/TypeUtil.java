package io.scalecube.services;

import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;

public final class TypeUtil {

  private TypeUtil() {
    // Do not instantiate
  }

  public static String getTypeDescriptor(Object object) {
    return object != null ? object.getClass().getName() : null;
  }

  public static Type parseTypeDescriptor(String descriptor) {
    if (descriptor == null) {
      return null;
    }
    try {
      return Class.forName(descriptor);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Invalid or unknown type: " + descriptor, e);
    }
  }

  public static boolean isWildcardType(Type type) {
    return type == Object.class || type instanceof WildcardType;
  }
}
