package io.scalecube.services;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility for converting runtime objects to string type descriptors and parses such descriptors
 * back into {@link java.lang.reflect.Type} instances.
 *
 * <p>Intended for transferring type metadata inside {@link
 * io.scalecube.services.api.ServiceMessage} to support encoding/decoding.
 *
 * <p>Supported descriptor forms:
 *
 * <ul>
 *   <li>Primitive types
 *   <li>Fully-qualified class names
 *   <li>Arrays (including multi-dimensional)
 *   <li>Parameterized {@code List/Set/Map}
 * </ul>
 *
 * <p>Generic parameters are inferred from the first element ({@code List/Set}) or first entry
 * ({@code Map}). Empty collections produce raw class names without generics.
 *
 * <p>Descriptors do not preserve concrete collection implementations. Only {@code List/Set/Map}
 * interfaces are used in parameterized descriptors.
 *
 * <p>Descriptors containing "null" (e.g. due to null collection elements) are considered invalid
 * and cannot be parsed.
 */
public class TypeUtil {

  // Map for primitive types
  private static final Map<String, Class<?>> PRIMITIVE_CLASSES = new HashMap<>();

  static {
    PRIMITIVE_CLASSES.put("int", int.class);
    PRIMITIVE_CLASSES.put("long", long.class);
    PRIMITIVE_CLASSES.put("double", double.class);
    PRIMITIVE_CLASSES.put("float", float.class);
    PRIMITIVE_CLASSES.put("boolean", boolean.class);
    PRIMITIVE_CLASSES.put("char", char.class);
    PRIMITIVE_CLASSES.put("byte", byte.class);
    PRIMITIVE_CLASSES.put("short", short.class);
    PRIMITIVE_CLASSES.put("void", void.class);
  }

  /**
   * Returns string descriptor representing the runtime type of the given object.
   *
   * <p>Behavior:
   *
   * <ul>
   *   <li>null -> null
   *   <li>Arrays -> component type descriptor with "[]" per dimension
   *   <li>Non-empty {@code List/Set} -> {@code java.util.List/Set<element-type>}
   *   <li>Non-empty {@code Map} -> {@code java.util.Map<key-type, value-type>}
   *   <li>All other objects -> fully-qualified class name
   * </ul>
   *
   * <p>For collections, generic types are inferred from the first element or entry only. If the
   * first element or key/value is null, the descriptor will contain "null".
   *
   * @param object object to describe (may be null)
   * @return descriptor string or null
   */
  public static String getTypeDescriptor(Object object) {
    if (object == null) {
      return null;
    }

    final var clazz = object.getClass();

    if (clazz.isArray()) {
      return getArrayDescriptor(clazz);
    }

    if (List.class.isAssignableFrom(clazz) && object instanceof List<?> list && !list.isEmpty()) {
      String elementType = getTypeDescriptor(list.get(0));
      return "java.util.List<" + elementType + ">";
    }

    if (Set.class.isAssignableFrom(clazz) && object instanceof Set<?> set && !set.isEmpty()) {
      String elementType = getTypeDescriptor(set.iterator().next());
      return "java.util.Set<" + elementType + ">";
    }

    if (Map.class.isAssignableFrom(clazz) && object instanceof Map<?, ?> map && !map.isEmpty()) {
      Map.Entry<?, ?> entry = map.entrySet().iterator().next();
      String keyType = getTypeDescriptor(entry.getKey());
      String valueType = getTypeDescriptor(entry.getValue());
      return "java.util.Map<" + keyType + "," + valueType + ">";
    }

    return clazz.getName();
  }

  /**
   * Parses type descriptor string into {@link java.lang.reflect.Type} instance.
   *
   * <p>Recognized forms:
   *
   * <ul>
   *   <li>Primitive names
   *   <li>Fully-qualified class names
   *   <li>Array descriptors
   *   <li>Parameterized {@code List/Set/Map}
   * </ul>
   *
   * <p>Supports arbitrary nesting of the above forms.
   *
   * @param descriptor descriptor string (may be null)
   * @return {@link java.lang.reflect.Type} instance, or null if descriptor is null
   * @throws IllegalArgumentException if the descriptor is syntactically invalid or references an
   *     unknown type
   */
  public static Type parseTypeDescriptor(String descriptor) {
    if (descriptor == null) {
      return null;
    }

    if (descriptor.endsWith("[]")) {
      String componentName = descriptor.substring(0, descriptor.length() - 2);
      Type componentType = parseTypeDescriptor(componentName);
      return new GenericArrayTypeImpl(componentType);
    }

    if (descriptor.startsWith("java.util.List<")) {
      String innerType = extractGeneric(descriptor);
      Type elementType = parseTypeDescriptor(innerType);
      return new ParameterizedTypeImpl(List.class, elementType);
    }

    if (descriptor.startsWith("java.util.Set<")) {
      String innerType = extractGeneric(descriptor);
      Type elementType = parseTypeDescriptor(innerType);
      return new ParameterizedTypeImpl(Set.class, elementType);
    }

    if (descriptor.startsWith("java.util.Map<")) {
      String[] types = extractMapGenerics(descriptor);
      Type keyType = parseTypeDescriptor(types[0]);
      Type valueType = parseTypeDescriptor(types[1]);
      return new ParameterizedTypeImpl(Map.class, keyType, valueType);
    }

    return loadClass(descriptor);
  }

  private static String getArrayDescriptor(Class<?> arrayClass) {
    return arrayClass.isArray()
        ? getArrayDescriptor(arrayClass.getComponentType()) + "[]"
        : arrayClass.getName();
  }

  private static String extractGeneric(String descriptor) {
    int start = descriptor.indexOf('<') + 1;
    int end = findMatchingBracket(descriptor, start - 1);
    return descriptor.substring(start, end);
  }

  private static String[] extractMapGenerics(String descriptor) {
    String generics = extractGeneric(descriptor);
    int depth = 0;
    int splitPos = -1;
    for (int i = 0; i < generics.length(); i++) {
      char c = generics.charAt(i);
      if (c == '<') depth++;
      else if (c == '>') depth--;
      else if (c == ',' && depth == 0) {
        splitPos = i;
        break;
      }
    }
    return new String[] {generics.substring(0, splitPos), generics.substring(splitPos + 1)};
  }

  private static int findMatchingBracket(String s, int openPos) {
    int depth = 1;
    for (int i = openPos + 1; i < s.length(); i++) {
      if (s.charAt(i) == '<') depth++;
      else if (s.charAt(i) == '>') {
        depth--;
        if (depth == 0) return i;
      }
    }
    throw new IllegalArgumentException("Unmatched bracket");
  }

  private static Class<?> loadClass(String name) {
    if (PRIMITIVE_CLASSES.containsKey(name)) {
      return PRIMITIVE_CLASSES.get(name);
    }

    if (name == null || name.trim().isEmpty() || "null".equals(name)) {
      throw new IllegalArgumentException("Invalid type name: " + name);
    }

    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Invalid or unknown type: " + name, e);
    }
  }

  private record ParameterizedTypeImpl(Class<?> rawType, Type... actualTypeArguments)
      implements ParameterizedType {

    @Override
    public Type[] getActualTypeArguments() {
      return actualTypeArguments;
    }

    @Override
    public Type getRawType() {
      return rawType;
    }

    @Override
    public Type getOwnerType() {
      return null;
    }

    @Override
    public String getTypeName() {
      StringBuilder sb = new StringBuilder(rawType.getName());
      if (actualTypeArguments.length > 0) {
        sb.append('<');
        for (int i = 0; i < actualTypeArguments.length; i++) {
          if (i > 0) sb.append(", ");
          sb.append(actualTypeArguments[i].getTypeName());
        }
        sb.append('>');
      }
      return sb.toString();
    }
  }

  private record GenericArrayTypeImpl(Type componentType) implements GenericArrayType {

    @Override
    public Type getGenericComponentType() {
      return componentType;
    }

    @Override
    public String getTypeName() {
      return componentType.getTypeName() + "[]";
    }
  }
}
