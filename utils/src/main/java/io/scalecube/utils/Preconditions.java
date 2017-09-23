package io.scalecube.utils;

import javax.annotation.Nullable;

public class Preconditions {

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param errorMessageTemplate a template for the exception message should the check fail. The message is formed by
   *        replacing each {@code %s} placeholder in the template with an argument. These are matched by position - the
   *        first {@code %s} gets {@code
   *     errorMessageArgs[0]}, etc. Unmatched arguments will be appended to the formatted message in square braces.
   *        Unmatched placeholders will be left as-is.
   * @param errorMessageArgs the arguments to be substituted into the message template. Arguments are converted to
   *        strings using {@link String#valueOf(Object)}.
   * @throws IllegalArgumentException if {@code expression} is false
   * @throws NullPointerException if the check fails and either {@code errorMessageTemplate} or {@code errorMessageArgs}
   *         is null (don't let this happen)
   */
  public static void checkArgument(
      boolean expression,
      @Nullable String errorMessageTemplate,
      @Nullable Object... errorMessageArgs) {
    if (!expression) {
      throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param errorMessage the exception message to use if the check fails; will be converted to a string using
   *        {@link String#valueOf(Object)}
   * @throws IllegalArgumentException if {@code expression} is false
   */
  public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(String.valueOf(errorMessage));
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   *
   * @param expression a boolean expression
   * @throws IllegalArgumentException if {@code expression} is false
   */
  public static void checkArgument(boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling instance, but not involving any parameters to
   * the calling method.
   *
   * @param expression a boolean expression
   * @throws IllegalStateException if {@code expression} is false
   * @see Verify#verify Verify.verify()
   */
  public static void checkState(boolean expression) {
    if (!expression) {
      throw new IllegalStateException();
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling instance, but not involving any parameters to
   * the calling method.
   *
   * @param expression a boolean expression
   * @param errorMessage the exception message to use if the check fails; will be converted to a string using
   *        {@link String#valueOf(Object)}
   * @throws IllegalStateException if {@code expression} is false
   * @see Verify#verify Verify.verify()
   */
  public static void checkState(boolean expression, @Nullable Object errorMessage) {
    if (!expression) {
      throw new IllegalStateException(String.valueOf(errorMessage));
    }
  }

  /**
   * Substitutes each {@code %s} in {@code template} with an argument. These are matched by position: the first
   * {@code %s} gets {@code args[0]}, etc. If there are more arguments than placeholders, the unmatched arguments will
   * be appended to the end of the formatted message in square braces.
   *
   * @param template a non-null string containing 0 or more {@code %s} placeholders.
   * @param args the arguments to be substituted into the message template. Arguments are converted to strings using
   *        {@link String#valueOf(Object)}. Arguments can be null.
   */
  // Note that this is somewhat-improperly used from Verify.java as well.
  static String format(String template, @Nullable Object... args) {
    template = String.valueOf(template); // null -> "null"

    // start substituting the arguments into the '%s' placeholders
    StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
    int templateStart = 0;
    int index = 0;
    while (index < args.length) {
      int placeholderStart = template.indexOf("%s", templateStart);
      if (placeholderStart == -1) {
        break;
      }
      builder.append(template.substring(templateStart, placeholderStart));
      builder.append(args[index++]);
      templateStart = placeholderStart + 2;
    }
    builder.append(template.substring(templateStart));

    // if we run out of placeholders, append the extra args in square braces
    if (index < args.length) {
      builder.append(" [");
      builder.append(args[index++]);
      while (index < args.length) {
        builder.append(", ");
        builder.append(args[index++]);
      }
      builder.append(']');
    }

    return builder.toString();
  }
}
