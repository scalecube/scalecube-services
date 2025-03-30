package io.scalecube.services.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@Repeatable(AllowedRoles.class)
public @interface AllowedRole {

  /**
   * Role name.
   *
   * @return role name
   */
  String name();

  /**
   * Allowed permissions.
   *
   * @return permissions
   */
  String[] permissions();
}
