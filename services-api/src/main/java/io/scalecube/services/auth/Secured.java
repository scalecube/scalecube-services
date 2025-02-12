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
   * Allowed roles.
   *
   * @return roles.
   */
  String[] roles() default {};

  /**
   * Allowed permissions.
   *
   * @return permissions.
   */
  String[] permissions() default {};
}
