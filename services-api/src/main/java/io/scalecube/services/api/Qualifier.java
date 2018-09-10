package io.scalecube.services.api;

/** Qualifier utility class. */
public final class Qualifier {

  public static final String DELIMITER = "/";

  public static final String ERROR_NAMESPACE = "io.scalecube.services.error";

  /**
   * Builds error qualifier.
   *
   * @param action qualifier action.
   * @return constructed qualifier string.
   */
  public static String asError(int action) {
    return asString(ERROR_NAMESPACE, Integer.toString(action));
  }

  /**
   * Builds qualifier string out of given namespace and action.
   *
   * @param namespace qualifier namespace.
   * @param action qualifier action.
   * @return constructed qualifier.
   */
  public static String asString(String namespace, String action) {
    return DELIMITER + namespace + DELIMITER + action;
  }

  /**
   * Extracts qualifier namespace part from given qualifier string.
   *
   * @param qualifierAsString qualifier string.
   * @return qualifier namespace.
   */
  public static String getQualifierNamespace(String qualifierAsString) {
    int pos = qualifierAsString.indexOf(DELIMITER, 1);
    if (pos == -1) {
      throw new IllegalArgumentException("Wrong qualifier format: '" + qualifierAsString + "'");
    }
    return qualifierAsString.substring(1, pos);
  }

  /**
   * Extracts qualifier action part from given qualifier string.
   *
   * @param qualifierAsString qualifier string.
   * @return qualifier action.
   */
  public static String getQualifierAction(String qualifierAsString) {
    int pos = qualifierAsString.lastIndexOf(DELIMITER);
    if (pos == -1) {
      throw new IllegalArgumentException("Wrong qualifier format: '" + qualifierAsString + "'");
    }
    return qualifierAsString.substring(pos + 1, qualifierAsString.length());
  }
}
