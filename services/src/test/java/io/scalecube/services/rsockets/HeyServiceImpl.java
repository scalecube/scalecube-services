package io.scalecube.services.rsockets;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class HeyServiceImpl implements HeyService {

    @Override
    public Mono<HeyPayload> revert(HeyPayload req) {
        return Mono.just(new HeyPayload(new StringBuffer(req.getText()).reverse().toString()));
    }

    @Override
    public Flux<HeyPayload> revertStream(Flux<HeyPayload> reqStream) {
        return null;
    }

}
