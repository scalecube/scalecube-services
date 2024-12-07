package io.scalecube.services.api;

import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DynamicQualifierTest {

  @Test
  void testNoMatches() {
    final var qualifier = new DynamicQualifier("v1/foo/:foo/bar/:bar");
    Assertions.assertNull(qualifier.match("v1/foo/bar"));
  }

  @Test
  void testIllegalArgument() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DynamicQualifier("v1/foo/bar"));
  }

  @Test
  void testMatchSinglePathVariable() {
    final var userName = UUID.randomUUID().toString();
    final var qualifier = new DynamicQualifier("v1/foo/bar/:userName");
    final var map = qualifier.match("v1/foo/bar/" + userName);
    Assertions.assertNotNull(map);
    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals(userName, map.get("userName"));
  }

  @Test
  void testMatchMultiplePathVariables() {
    final var qualifier = new DynamicQualifier("v1/foo/:foo/bar/:bar/baz/:baz");
    final var map = qualifier.match("v1/foo/123/bar/456/baz/678");
    Assertions.assertNotNull(map);
    Assertions.assertEquals(3, map.size());
    Assertions.assertEquals("123", map.get("foo"));
    Assertions.assertEquals("456", map.get("bar"));
    Assertions.assertEquals("678", map.get("baz"));
  }
}
