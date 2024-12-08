package io.scalecube.services.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class DynamicQualifierTest {

  @Test
  void testIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> new DynamicQualifier("v1/foo/bar"));
  }

  @Test
  void testNoMatches() {
    final var qualifier = new DynamicQualifier("v1/foo/:foo/bar/:bar");
    assertNull(qualifier.matchQualifier("v1/foo/bar"));
  }

  @Test
  void testStrictMatching() {
    final var qualifier = new DynamicQualifier("v1/foo/:foo");
    assertNotNull(qualifier.matchQualifier("v1/foo/123"));
    assertNull(qualifier.matchQualifier("v1/foo/123/bar/456/baz/678"));
  }

  @Test
  void testEquality() {
    final var qualifier1 = new DynamicQualifier("v1/foo/:foo/bar/:bar");
    final var qualifier2 = new DynamicQualifier("v1/foo/:foo/bar/:bar");
    assertEquals(qualifier1, qualifier2);
  }

  @Test
  void testMatchSinglePathVariable() {
    final var userName = UUID.randomUUID().toString();
    final var qualifier = new DynamicQualifier("v1/foo/bar/:userName");
    final var map = qualifier.matchQualifier("v1/foo/bar/" + userName);
    assertNotNull(map);
    assertEquals(1, map.size());
    assertEquals(userName, map.get("userName"));
  }

  @Test
  void testMatchMultiplePathVariables() {
    final var qualifier = new DynamicQualifier("v1/foo/:foo/bar/:bar/baz/:baz");
    final var map = qualifier.matchQualifier("v1/foo/123/bar/456/baz/678");
    assertNotNull(map);
    assertEquals(3, map.size());
    assertEquals("123", map.get("foo"));
    assertEquals("456", map.get("bar"));
    assertEquals("678", map.get("baz"));
  }
}
