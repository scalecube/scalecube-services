package io.scalecube.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that an annotated method is a service method available via Service Fabric framework.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface ServiceMethod {

  /**
   * Name of the method. In case if not provided will be used the result of annotated method, but with this annotation
   * you can override method result so it won't depend on specific result in the code.
   */
  String value() default "";

}
