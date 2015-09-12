package io.servicefabric.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that an annotated class is an Service Fabric service object.
 * 
 * @author Anton Kharenko
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Service {

  /**
   * Name of the service. If not specified service class name will be used.
   */
  String value() default "";

}
