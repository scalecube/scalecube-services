package io.scalecube.services.api;

import java.util.Objects;

public final class Qualifier {

  public static final String Q_DELIMITER = "/";
  public static final String Q_NAMESPACE = "io.scalecube.streams";
  public static final String Q_ERROR_NAMESPACE = Q_NAMESPACE + ".onError";
  public static final String Q_COMPLETED_NAMESPACE = Q_NAMESPACE + ".onCompleted";

  // qualifier for generic error
  public static final Qualifier Q_GENERAL_FAILURE = Qualifier.fromString(Q_ERROR_NAMESPACE + Q_DELIMITER + 500);
  // qualifier for onCompleted event
  public static final Qualifier Q_ON_COMPLETED = Qualifier.fromString(Q_COMPLETED_NAMESPACE + Q_DELIMITER + 1);

  private final String namespace;
  private final String action;
  private final String stringValue; // calculated

  public static Qualifier error(int action) {
    return error(Integer.toString(action));
  }

  public static Qualifier error(String action) {
    return new Qualifier(Q_ERROR_NAMESPACE, action);
  }

  /**
   * Basic constructor with namespace and action.
   */
  public Qualifier(String namespace, String action) {
    this.namespace = namespace;

    this.action = action;
    if (action == null) {
      this.stringValue = namespace;
    } else {
      this.stringValue = namespace + Q_DELIMITER + action;
    }
  }

  public String getNamespace() {
    return namespace;
  }

  public String getAction() {
    return action;
  }

  public String asString() {
    return stringValue;
  }

  public boolean isEquals(String qualifier) {
    return stringValue.equals(qualifier);
  }

  public boolean isEqualsIgnoreCase(String qualifier) {
    return stringValue.equalsIgnoreCase(qualifier);
  }

  /**
   * Constructs qualifier object from string.
   */
  public static Qualifier fromString(String qualifierAsString) throws IllegalArgumentException {
    int indexOf = qualifierAsString.indexOf(Q_DELIMITER);
    if (indexOf == -1) {
      // whole string is namespace
      return new Qualifier(qualifierAsString, null);
    }
    if (indexOf + 1 >= qualifierAsString.length()) {
      String namespace = qualifierAsString.substring(0, indexOf);
      if (namespace.isEmpty()) {
        throw new IllegalArgumentException(qualifierAsString);
      }
      return new Qualifier(namespace, null);
    }
    String namespace = qualifierAsString.substring(0, indexOf);
    String action = qualifierAsString.substring(indexOf + 1);
    return new Qualifier(namespace, action);
  }

  /**
   * @return qualifier namespace.
   */
  public static String getQualifierNamespace(String qualifierAsString) {
    int pos = qualifierAsString.indexOf(Q_DELIMITER);
    if (pos == -1) {
      return qualifierAsString;
    }
    return qualifierAsString.substring(0, pos);
  }

  /**
   * @return qualifier action.
   */
  public static String getQualifierAction(String qualifierAsString) {
    int pos = qualifierAsString.indexOf(Q_DELIMITER);
    if (pos == -1) {
      return qualifierAsString;
    }
    return qualifierAsString.substring(pos + 1, qualifierAsString.length());
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Qualifier qualifier = (Qualifier) obj;

    return Objects.equals(stringValue, qualifier.stringValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stringValue);
  }
}
