package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.registry.ServiceRegistryImpl;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Request data of an incoming payload is owned by the method invoker once looked up: it is
 * released on decode. {@link RSocketImpl} must not release it again, whatever the response.
 */
class RSocketImplTest {

  private final ServiceMessageCodec codec = new ServiceMessageCodec();

  private RSocketImpl cut;

  @BeforeEach
  void beforeEach() {
    final ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
    serviceRegistry.registerService(
        ServiceInfo.fromServiceInstance(new ReleaseServiceImpl())
            .errorMapper(DefaultErrorMapper.INSTANCE)
            .dataDecoder(new ServiceMessageByteBufDataDecoder())
            .build());
    cut = new RSocketImpl(Principal.NULL_PRINCIPAL, codec, serviceRegistry);
  }

  @Test
  void requestResponseSuccessReleasesRequestDataOnce() {
    final RecyclingByteBuf data = data("\"hello\"");

    StepVerifier.create(cut.requestResponse(payload("release/echo", data)).map(this::toMessage))
        .assertNext(response -> assertTrue(!response.isError(), "error response"))
        .verifyComplete();

    assertEquals(1, data.deallocations(), "request data deallocations");
  }

  @Test
  void requestResponseErrorReleasesRequestDataOnce() {
    final RecyclingByteBuf data = data("\"hello\"");

    StepVerifier.create(cut.requestResponse(payload("release/fail", data)).map(this::toMessage))
        .assertNext(response -> assertTrue(response.isError(), "error response"))
        .verifyComplete();

    assertEquals(1, data.deallocations(), "request data deallocations");
  }

  @Test
  void requestStreamErrorReleasesRequestDataOnce() {
    final RecyclingByteBuf data = data("\"hello\"");

    StepVerifier.create(cut.requestStream(payload("release/failMany", data)).map(this::toMessage))
        .assertNext(response -> assertTrue(response.isError(), "error response"))
        .verifyComplete();

    assertEquals(1, data.deallocations(), "request data deallocations");
  }

  @Test
  void decodeErrorReleasesRequestDataOnce() {
    final RecyclingByteBuf data = data("{not json");

    StepVerifier.create(cut.requestResponse(payload("release/echo", data)).map(this::toMessage))
        .assertNext(response -> assertTrue(response.isError(), "error response"))
        .verifyComplete();

    assertEquals(1, data.deallocations(), "request data deallocations");
  }

  @Test
  void emptyDataSuccessReleasesRequestDataOnce() {
    final RecyclingByteBuf data = data("");

    StepVerifier.create(cut.requestResponse(payload("release/echo", data)).map(this::toMessage))
        .assertNext(response -> assertTrue(!response.isError(), "error response"))
        .verifyComplete();

    assertEquals(1, data.deallocations(), "request data deallocations");
  }

  @Test
  void unknownServiceReleasesRequestDataOnce() {
    final RecyclingByteBuf data = data("\"hello\"");

    StepVerifier.create(cut.requestResponse(payload("release/unknown", data)))
        .expectError()
        .verify();

    assertEquals(1, data.deallocations(), "request data deallocations");
  }

  private static RecyclingByteBuf data(String json) {
    final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    final RecyclingByteBuf data = new RecyclingByteBuf(bytes.length);
    data.writeBytes(bytes);
    return data;
  }

  private Payload payload(String qualifier, RecyclingByteBuf data) {
    final ServiceMessage message = ServiceMessage.builder().qualifier(qualifier).data(data).build();
    return codec.encodeAndTransform(message, ByteBufPayload::create);
  }

  private ServiceMessage toMessage(Payload payload) {
    try {
      return codec.decode(payload.sliceData().retain(), payload.sliceMetadata().retain());
    } finally {
      payload.release();
    }
  }

  @Service("release")
  public interface ReleaseService {

    @ServiceMethod
    Mono<String> echo(String request);

    @ServiceMethod
    Mono<String> fail(String request);

    @ServiceMethod
    Flux<String> failMany(String request);
  }

  public static class ReleaseServiceImpl implements ReleaseService {

    @Override
    public Mono<String> echo(String request) {
      return Mono.justOrEmpty(request).defaultIfEmpty("empty");
    }

    @Override
    public Mono<String> fail(String request) {
      return Mono.error(new IllegalStateException("fail"));
    }

    @Override
    public Flux<String> failMany(String request) {
      return Flux.error(new IllegalStateException("fail"));
    }
  }
}
