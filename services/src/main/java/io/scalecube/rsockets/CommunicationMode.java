package io.scalecube.rsockets;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Optional;


public enum CommunicationMode {
    FIRE_N_FORGET,
    MONO,
    REQ_STREAM,
    BIDIRECTIONAL;

    //TODO: handle invalid method definition possible failures [sergeyr]
    static Optional<CommunicationMode> of(Method m) {
        Class<?> returnType = m.getReturnType();
        if (Void.TYPE == returnType) {
            return Optional.of(FIRE_N_FORGET);
        } else if (Mono.class == returnType) {
            return Optional.of(MONO);
        } else if (Flux.class == returnType) {
            Class<?> reqType = m.getParameterTypes()[0];
            if (Flux.class == reqType) return Optional.of(BIDIRECTIONAL);
            else return Optional.of(REQ_STREAM);
        } else {
            throw new UnsupportedOperationException("Service method must be one of the following patterns:FIRE_N_FORGET, MONO, REQ_STREAM, BIDIRECTIONAL");
        }

    }
}
