package io.scalecube.services;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Miscellaneous {@code java.lang.Class} utility methods.
 * */
public class ClassUtils {

  private ClassUtils() {
    // Do not instantiate
  }

  /**
   * Map with primitive wrapper type as key and corresponding primitive type as value, for example:
   * Integer.class -> int.class.
   */
  private static final Map<Class<?>, Class<?>> primitiveWrapperTypeMap = new IdentityHashMap<>(8);

  /**
   * Map with primitive type as key and corresponding wrapper type as value, for example: int.class
   * -> Integer.class.
   */
  private static final Map<Class<?>, Class<?>> primitiveTypeToWrapperMap = new IdentityHashMap<>(8);

  static {
    primitiveWrapperTypeMap.put(Boolean.class, boolean.class);
    primitiveWrapperTypeMap.put(Byte.class, byte.class);
    primitiveWrapperTypeMap.put(Character.class, char.class);
    primitiveWrapperTypeMap.put(Double.class, double.class);
    primitiveWrapperTypeMap.put(Float.class, float.class);
    primitiveWrapperTypeMap.put(Integer.class, int.class);
    primitiveWrapperTypeMap.put(Long.class, long.class);
    primitiveWrapperTypeMap.put(Short.class, short.class);

    // Map entry iteration is less expensive to initialize than forEach with lambdas
    for (Map.Entry<Class<?>, Class<?>> entry : primitiveWrapperTypeMap.entrySet()) {
      primitiveTypeToWrapperMap.put(entry.getValue(), entry.getKey());
    }
  }

  /**
   * Check if the right-hand side type may be assigned to the left-hand side type, assuming setting
   * by reflection. Considers primitive wrapper classes as assignable to the corresponding primitive
   * types.
   *
   * @param lhsType the target type
   * @param rhsType the value type that should be assigned to the target type
   * @return if the target type is assignable from the value type // * @see TypeUtils#isAssignable
   */
  public static boolean isAssignable(Class<?> lhsType, Class<?> rhsType) {
    Objects.requireNonNull(lhsType, "Left-hand side type must not be null");
    Objects.requireNonNull(rhsType, "Right-hand side type must not be null");
    if (lhsType.isAssignableFrom(rhsType)) {
      return true;
    }
    if (lhsType.isPrimitive()) {
      return lhsType == primitiveWrapperTypeMap.get(rhsType);
    } else {
      Class<?> resolvedWrapper = primitiveTypeToWrapperMap.get(rhsType);
      return resolvedWrapper != null && lhsType.isAssignableFrom(resolvedWrapper);
    }
  }
}
