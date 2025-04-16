package io.scalecube.services.auth;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used to mark that service or service method is protected by authentication
 * mechanism.
 */
@Documented
@Target({METHOD, TYPE})
@Retention(RUNTIME)
public @interface Secured {

  /**
   * Returns whether service method must apply standard security check behavior.
   *
   * @return {@code true} if service must apply standard security check behavior, {@code false}
   *     otherwise
   */
  boolean deferSecured() default true;
}
