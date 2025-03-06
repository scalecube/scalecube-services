package io.scalecube.services.annotations;

import io.scalecube.services.routing.Router;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(value = {ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface Inject {

  /**
   * Router class to be used.
   *
   * @return router class
   */
  Class<? extends Router> router() default Router.class;
}
