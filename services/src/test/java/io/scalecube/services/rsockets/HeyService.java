package io.scalecube.services.rsockets;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public interface HeyService {

    @ServiceMethod
    Mono<HeyPayload> revert(HeyPayload req);

    @ServiceMethod
    Flux<HeyPayload> revertStream(Flux<HeyPayload> reqStream);
}
