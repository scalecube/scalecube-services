package io.scalecube.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Contains tags associated with {@link Service} or {@link ServiceMethod}.
 *
 * @see Tag
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE})
public @interface Tags {

  /**
   * Returns array of associated tags.
   *
   * @return array of tags
   */
  Tag[] value();
}
