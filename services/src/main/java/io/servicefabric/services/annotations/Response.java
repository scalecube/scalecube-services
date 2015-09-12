package io.servicefabric.services.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Service method response definition. This annotation can be used to specify or override message type and qualifier.
 *
 * @author Konstantin Shchepanovskyi
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Response {

	/**
	 * Override {@code action} part of message qualifier. Default value is method name + {@code Reply} suffix.
	 */
	String value();

}
