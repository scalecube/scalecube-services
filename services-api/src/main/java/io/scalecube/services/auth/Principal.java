package io.scalecube.services.auth;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** This annotation is used to mark input parameter of service method as principal. */
@Documented
@Target(PARAMETER)
@Retention(RUNTIME)
public @interface Principal {}
