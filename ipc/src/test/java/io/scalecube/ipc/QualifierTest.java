package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class QualifierTest {

  public static final String CONTEXT_NAMESPACE = "io.scalecube.context";

  @Test
  public void testNewQualifierWithoutVersion() throws Exception {
    Qualifier q = new Qualifier("namespace", "action");
    assertEquals("namespace/action", q.asString());
    assertEquals("namespace", q.getNamespace());
    assertEquals("action", q.getAction());
  }

  @Test
  public void testNewQualifierWithVersionAsPartOfAction() throws Exception {
    Qualifier q = new Qualifier("namespace", "action/1.0");
    assertEquals("namespace/action/1.0", q.asString());
    assertEquals("namespace", q.getNamespace());
    assertEquals("action/1.0", q.getAction());
  }

  @Test
  public void testNewQualifierWithNullAction() throws Exception {
    Qualifier q = new Qualifier("namespace", null);
    assertEquals("namespace", q.asString());
    assertEquals("namespace", q.getNamespace());
    assertNull(q.getAction());
  }

  @Test
  public void testFromStringQualifierForDefaultMethod() {
    Qualifier qualifier = Qualifier.fromString("io.scalecube.context");
    assertEquals(CONTEXT_NAMESPACE, qualifier.getNamespace());
    assertNull(qualifier.getAction());
  }

  @Test
  public void testFromStringQualifierForDefaultMethodWithSlashAfterNamespace() {
    // valid, first slash is delimiter
    Qualifier qualifier = Qualifier.fromString("io.scalecube.context/");
    assertEquals(CONTEXT_NAMESPACE, qualifier.getNamespace());
    assertNull(qualifier.getAction());
  }

  @Test
  public void testFromStringQualifierWithSlashAsAction() {
    // valid, second slash is action
    Qualifier qualifier = Qualifier.fromString("io.scalecube.context//");
    assertEquals(CONTEXT_NAMESPACE, qualifier.getNamespace());
    assertEquals("/", qualifier.getAction());
  }

  @Test
  public void testFromStringFromStringWithMalformedVersion() {
    Qualifier q = Qualifier.fromString("io.scalecube.context/yadayada/y");
    assertEquals(CONTEXT_NAMESPACE, q.getNamespace());
    assertEquals("yadayada/y", q.getAction());
  }

  @Test
  public void testFromStringFromStringWithVersion() {
    Qualifier q = Qualifier.fromString("io.scalecube.context/yadayada/1.0");
    assertEquals(CONTEXT_NAMESPACE, q.getNamespace());
    assertEquals("yadayada/1.0", q.getAction());
  }
}
