package io.scalecube.services.methods;

import io.scalecube.services.api.ServiceMessage;
import java.util.function.BiFunction;

@FunctionalInterface
public interface ServiceMessageDataDecoder
    extends BiFunction<ServiceMessage, Class<?>, ServiceMessage> {}
