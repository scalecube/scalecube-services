package io.scalecube.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Subscriber {

  /**
   * Event type to subscribe on.
   *
   * @return event type
   */
  Class<?> value() default Object.class;
}
