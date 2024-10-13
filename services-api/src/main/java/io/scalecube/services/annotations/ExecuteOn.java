package io.scalecube.services.annotations;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used to mark that particular service method or all service methods will be
 * executed in the specified scheduler.
 */
@Documented
@Target({METHOD, TYPE})
@Retention(RUNTIME)
public @interface ExecuteOn {

  /**
   * Returns {@link reactor.core.scheduler.Scheduler} spec, in the format: {@code
   * <scheduler_name>:<scheduler_constructor_spec>}
   *
   * @return scheduler spec
   */
  String value();
}
