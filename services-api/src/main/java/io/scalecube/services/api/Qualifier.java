package io.scalecube.services.api;

/** Qualifier utility class. */
public final class Qualifier {

  public static final String DELIMITER = "/";

  /**
   * Builds qualifier string out of given namespace and action.
   *
   * @param namespace qualifier namespace.
   * @param action qualifier action.
   * @return constructed qualifier.
   */
  public static String asString(String namespace, String action) {
    return namespace + DELIMITER + action;
  }
}
