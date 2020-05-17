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
    return namespace + DELIMITER + action;
  }

  /**
   * Extracts qualifier namespace part from given qualifier string.
   *
   * @param qualifierAsString qualifier string.
   * @return qualifier namespace.
   */
  public static String getQualifierNamespace(String qualifierAsString) {
    // If qualifier starts with DELIMITER it's old format, if not then it's new format without
    // DELIMITER in the beginning
    int namespacePos = qualifierAsString.startsWith(DELIMITER) ? 1 : 0;
    int pos = qualifierAsString.indexOf(DELIMITER, namespacePos);
    if (pos == -1) {
      throw new IllegalArgumentException("Wrong qualifier format: '" + qualifierAsString + "'");
    }
    return qualifierAsString.substring(namespacePos, pos);
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
    return qualifierAsString.substring(pos + 1);
  }
}
