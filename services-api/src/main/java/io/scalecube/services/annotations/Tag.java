package io.scalecube.services.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Key-value entry to provide additional information about service itself or method. This annotation
 * will be applied only in combination with {@link Service} or {@link ServiceMethod}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE})
@Repeatable(Tags.class)
public @interface Tag {

  /**
   * Returns the key corresponding to this entry.
   *
   * @return key
   */
  String key();

  /**
   * Returns the value corresponding to this entry.
   *
   * @return value
   */
  String value();
}
