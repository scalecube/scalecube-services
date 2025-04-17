package io.scalecube.services.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface AllowedRoles {

  /**
   * Returns array of associated allowed roles.
   *
   * @return allowed roles
   */
  AllowedRole[] value();
}
