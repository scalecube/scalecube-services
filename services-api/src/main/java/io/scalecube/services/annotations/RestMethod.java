package io.scalecube.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Indicates that annotated method is a REST service method. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface RestMethod {

  /**
   * Name of the HTTP method. Supported methods: GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS,
   * TRACE.
   *
   * @return http method
   */
  String value();
}
