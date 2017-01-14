
package io.scalecube.services.annotations;

import io.scalecube.services.routing.Router;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER,ElementType.FIELD})
public @interface ServiceProxy {
 /**
   * proxy router. If not specified default router will be used.
   */
  Class<? extends Router> router();
  
  /**
   * proxy request timeout in milliseconds. default value 0.
   */
  long timeout();
}
