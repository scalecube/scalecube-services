package io.scalecube.services.annotations;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used to mark the method which will be executed after constructing of service
 * and dependency injection is done. <br>
 * Scalecube services doesn't support {@link javax.annotation.PostConstruct} since Java API
 * Specification for it has strict limitation for annotated method.
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
public @interface AfterConstruct {}
