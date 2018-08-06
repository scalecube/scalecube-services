package io.scalecube.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface ContentType {

  String DEFAULT = "application/json";

  /**
   * Content type as String.
   * 
   * @return the content type
   */
  String value() default DEFAULT;
}
