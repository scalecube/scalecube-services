package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TypeUtilsTest {

  @Test
  void testPrimitiveIntArray() {
    final var arr = new int[] {};
    final var desc = TypeUtils.getTypeDescriptor(arr);
    assertEquals("int[]", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(GenericArrayType.class, type);
    assertEquals("int", ((GenericArrayType) type).getGenericComponentType().getTypeName());
  }

  @Test
  void testPrimitiveLongArray() {
    final var arr = new long[] {};
    final var desc = TypeUtils.getTypeDescriptor(arr);
    assertEquals("long[]", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(GenericArrayType.class, type);
    assertEquals("long", ((GenericArrayType) type).getGenericComponentType().getTypeName());
  }

  @Test
  void testIntegerArray() {
    final var arr = new Integer[0];
    final var desc = TypeUtils.getTypeDescriptor(arr);
    assertEquals(Integer.class.getName() + "[]", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(GenericArrayType.class, type);
    assertEquals(
        Integer.class.getName(), ((GenericArrayType) type).getGenericComponentType().getTypeName());
  }

  @Test
  void testLongArray() {
    final var arr = new Long[0];
    final var desc = TypeUtils.getTypeDescriptor(arr);
    assertEquals(Long.class.getName() + "[]", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(GenericArrayType.class, type);
    assertEquals(
        Long.class.getName(), ((GenericArrayType) type).getGenericComponentType().getTypeName());
  }

  @Test
  void testStringArray() {
    final var arr = new String[0];
    final var desc = TypeUtils.getTypeDescriptor(arr);
    assertEquals(String.class.getName() + "[]", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(GenericArrayType.class, type);
    assertEquals(
        String.class.getName(), ((GenericArrayType) type).getGenericComponentType().getTypeName());
  }

  @Test
  void testObjectArray() {
    final var arr = new MyDto[0];
    final var desc = TypeUtils.getTypeDescriptor(arr);
    assertEquals(MyDto.class.getName() + "[]", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(GenericArrayType.class, type);
    assertEquals(
        MyDto.class.getName(), ((GenericArrayType) type).getGenericComponentType().getTypeName());
  }

  @Test
  void testEmptyArrayList() {
    final var list = new ArrayList<String>();

    final var desc = TypeUtils.getTypeDescriptor(list);
    assertEquals("java.util.ArrayList", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertEquals(ArrayList.class, type);
  }

  @Test
  void testIntegerList() {
    final var list = new ArrayList<Integer>();
    list.add(213);

    final var desc = TypeUtils.getTypeDescriptor(list);
    assertEquals("java.util.List<java.lang.Integer>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(List.class, pt.getRawType());
    assertEquals(Integer.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testLongList() {
    final var list = new ArrayList<Long>();
    list.add(213L);

    final var desc = TypeUtils.getTypeDescriptor(list);
    assertEquals("java.util.List<java.lang.Long>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(List.class, pt.getRawType());
    assertEquals(Long.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testStringList() {
    final var list = new ArrayList<String>();
    list.add("test");

    final var desc = TypeUtils.getTypeDescriptor(list);
    assertEquals("java.util.List<java.lang.String>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(List.class, pt.getRawType());
    assertEquals(String.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testObjectList() {
    final var list = new ArrayList<MyDto>();
    list.add(new MyDto());

    final var desc = TypeUtils.getTypeDescriptor(list);
    assertEquals("java.util.List<io.scalecube.services.TypeUtilsTest$MyDto>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(List.class, pt.getRawType());
    assertEquals(MyDto.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testEmptyHashSet() {
    final var set = new HashSet<String>();

    final var desc = TypeUtils.getTypeDescriptor(set);
    assertEquals("java.util.HashSet", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertEquals(HashSet.class, type);
  }

  @Test
  void testIntegerHashSet() {
    final var set = new HashSet<Integer>();
    set.add(213);

    final var desc = TypeUtils.getTypeDescriptor(set);
    assertEquals("java.util.Set<java.lang.Integer>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(Set.class, pt.getRawType());
    assertEquals(Integer.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testLongHashSet() {
    final var set = new HashSet<Long>();
    set.add(213L);

    final var desc = TypeUtils.getTypeDescriptor(set);
    assertEquals("java.util.Set<java.lang.Long>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(Set.class, pt.getRawType());
    assertEquals(Long.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testStringHashSet() {
    final var set = new HashSet<String>();
    set.add("test");

    final var desc = TypeUtils.getTypeDescriptor(set);
    assertEquals("java.util.Set<java.lang.String>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(Set.class, pt.getRawType());
    assertEquals(String.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testObjectHashSet() {
    final var set = new HashSet<MyDto>();
    set.add(new MyDto());

    final var desc = TypeUtils.getTypeDescriptor(set);
    assertEquals("java.util.Set<io.scalecube.services.TypeUtilsTest$MyDto>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    ParameterizedType pt = (ParameterizedType) type;
    assertEquals(Set.class, pt.getRawType());
    assertEquals(MyDto.class, pt.getActualTypeArguments()[0]);
  }

  @Test
  void testEmptyHashMap() {
    final var map = new HashMap<String, String>();

    final var desc = TypeUtils.getTypeDescriptor(map);
    assertEquals("java.util.HashMap", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertEquals(HashMap.class, type);
  }

  @Test
  void testMap() {
    final var map = new HashMap<Integer, MyDto>();
    map.put(1, new MyDto());

    final var desc = TypeUtils.getTypeDescriptor(map);
    assertEquals(
        "java.util.Map<java.lang.Integer,io.scalecube.services.TypeUtilsTest$MyDto>", desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    final var pt = (ParameterizedType) type;
    assertEquals(Map.class, pt.getRawType());
    assertEquals(Integer.class, pt.getActualTypeArguments()[0]);
    assertEquals(MyDto.class, pt.getActualTypeArguments()[1]);
  }

  @Test
  void testNestedGenerics() {
    final var list = new ArrayList<Map<String, MyDto>>();
    list.add(Map.of("key", new MyDto()));

    final var desc = TypeUtils.getTypeDescriptor(list);
    final var expected =
        "java.util.List<java.util.Map<java.lang.String,io.scalecube.services.TypeUtilsTest$MyDto>>";
    assertEquals(expected, desc);

    // Round trip verification
    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertInstanceOf(ParameterizedType.class, type);

    // Outer List
    final var listType = (ParameterizedType) type;
    assertEquals(List.class, listType.getRawType());

    // Inner Map
    final var inner = listType.getActualTypeArguments()[0];
    assertInstanceOf(ParameterizedType.class, inner);

    final var mapType = (ParameterizedType) inner;
    assertEquals(Map.class, mapType.getRawType());
    assertEquals(String.class, mapType.getActualTypeArguments()[0]);
    assertEquals(MyDto.class, mapType.getActualTypeArguments()[1]);
  }

  @Test
  void testSimpleObject() {
    final var dto = new MyDto();
    final var desc = TypeUtils.getTypeDescriptor(dto);
    assertEquals(MyDto.class.getName(), desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertEquals(MyDto.class, type);
  }

  @Test
  void testGenericDto() {
    final var dto = new MyBaseDto<MyDto>();
    final var desc = TypeUtils.getTypeDescriptor(dto);
    assertEquals(MyBaseDto.class.getName(), desc);

    final var type = TypeUtils.parseTypeDescriptor(desc);
    assertEquals(MyBaseDto.class, type);
  }

  static class MyDto {}

  static class MyBaseDto<T extends MyDto> {}
}
