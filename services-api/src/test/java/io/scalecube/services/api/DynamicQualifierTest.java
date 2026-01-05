package io.scalecube.services.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DynamicQualifierTest {

  @ValueSource(
      strings = {
        "",
        "v1",
        "v1/:/:",
        "v1/:param:/endpoints",
        "v1/namespace",
        "v1/namespace/:",
        "v1/namespace/p:",
        "v1/namespace/:::",
        "v1/this.is.namespace/foo/bar",
        "v1/this:is:namespace/foo/bar",
        "v1/namespace/f:oo/:b:ar",
        "v1/namespace/foo/:",
        "v1/namespace/:/bar",
        "v1/:bar:/${microservices:id}",
        "v1/${microservices:id}/:/bar",
        "v1/${microservices:id}/:b:a:r",
        "v1/${microservices:id}/foo/bar",
      })
  @ParameterizedTest
  void testIsNotDynamicQualifier(String input) {
    assertFalse(DynamicQualifier.isDynamicQualifier(input), "isNotDynamicQualifier");
  }

  @ValueSource(
      strings = {
        "v1/:p",
        "v1/:param",
        "v1/heart.beat/:param",
        "v1/:name/files",
        "v1/:name/endpoint.files",
        "v1/api/:name/:param",
        "v1/api/${microservices:id}/:param",
        "v1/api/:param/${microservices:id}",
        "v1/api/:name/${microservices:id}/:param"
      })
  @ParameterizedTest
  void testIsDynamicQualifier(String input) {
    assertTrue(DynamicQualifier.isDynamicQualifier(input), "isDynamicQualifier");
  }

  @Test
  void testNoMatches() {
    final var qualifier = DynamicQualifier.from("v1/this.is.namespace/foo/:foo/bar/:bar");
    assertNull(qualifier.matchQualifier("v1/this.is.namespace/foo/bar"));
  }

  @Test
  void testNoMatchesForEmptyValues() {
    final var qualifier = DynamicQualifier.from("v1/this.is.namespace/foo/:foo");
    assertNull(qualifier.matchQualifier("v1/this.is.namespace/foo/"));
  }

  @Test
  void testStrictMatching() {
    final var qualifier = DynamicQualifier.from("v1/this.is.namespace/foo/:foo");
    assertNotNull(qualifier.matchQualifier("v1/this.is.namespace/foo/123"));
    assertNull(qualifier.matchQualifier("v1/this.is.namespace/foo/123/bar/456/baz/678"));
  }

  @Test
  void testEquality() {
    final var qualifier1 = DynamicQualifier.from("v1/this.is.namespace/foo/:foo/bar/:bar");
    final var qualifier2 = DynamicQualifier.from("v1/this.is.namespace/foo/:foo/bar/:bar");
    assertEquals(qualifier1, qualifier2);
  }

  @Test
  void testMatchSinglePathParam() {
    final var userName = UUID.randomUUID().toString();
    final var qualifier = DynamicQualifier.from("v1/this.is.namespace/foo/bar/:userName");
    final var map = qualifier.matchQualifier("v1/this.is.namespace/foo/bar/" + userName);
    assertNotNull(map);
    assertEquals(1, map.size());
    assertEquals(userName, map.get("userName"));
  }

  @Test
  void testMatchMultiplePathParams() {
    final var qualifier = DynamicQualifier.from("v1/this.is.namespace/foo/:foo/bar/:bar/baz/:baz");
    final var map = qualifier.matchQualifier("v1/this.is.namespace/foo/123/bar/456/baz/678");
    assertNotNull(map);
    assertEquals(3, map.size());
    assertEquals("123", map.get("foo"));
    assertEquals("456", map.get("bar"));
    assertEquals("678", map.get("baz"));
  }
}
