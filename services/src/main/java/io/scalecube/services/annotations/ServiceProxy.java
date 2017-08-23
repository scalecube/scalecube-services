package io.scalecube.services.annotations;

import io.scalecube.services.routing.Router;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;


@Target(value = {ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface ServiceProxy {

  /**
   * Defines the router to be used when creating this proxy by default empty Router.
   * 
   * @return Router type.
   */
  Class<? extends Router> router() default Router.class;

  /**
   * Timeout while waiting for response from remote service by default is 3 seconds.
   * 
   * @return the expected timeout.
   */
  int timeout() default 3;

  /**
   * TimeUnit for the timeout by default TimeUnit.SECONDS.
   * 
   * @return time unit.
   */
  TimeUnit timeUnit() default TimeUnit.SECONDS;

}
