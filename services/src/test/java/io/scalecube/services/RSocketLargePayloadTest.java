package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

/**
 * Executable spec for the RSocket transport's large-payload handling, modeled on a production
 * incident where a service returned an unbounded result set as a single RSocket response. It
 * documents, as runnable cases, what happens for every payload size under every configuration.
 *
 * <h2>The two RSocket limits</h2>
 *
 * <ul>
 *   <li><b>Single-frame cap</b> — {@value #MAX_FRAME_LENGTH} bytes ({@code 2^24 - 1}). RSocket's
 *       frame-length header is 24 bits, so no single (non-fragmented) frame can be larger. Fixed by
 *       the protocol; it cannot be raised. This is the only limit these tests must use a real
 *       ~16&nbsp;MB payload to exercise.
 *   <li><b>Reassembly cap</b> — {@code maxInboundPayloadSize} (default ~2&nbsp;GiB, minimum 64&nbsp;B).
 *       With fragmentation on, a payload is split into frames and reassembled on the receiver up to
 *       this size.
 * </ul>
 *
 * <h2>The transport knobs (on {@link RSocketServiceTransport})</h2>
 *
 * <ul>
 *   <li>{@link RSocketServiceTransport#mtu(int) mtu} — enable fragmentation so a payload larger than
 *       the single-frame cap can be sent in pieces.
 *   <li>{@link RSocketServiceTransport#maxMessageSize(int) maxMessageSize} — the high-watermark. It
 *       (a) bounds outbound encoding via a <em>streaming</em> guard that aborts the instant the byte
 *       count crosses the limit — so an oversized message is never fully materialized and cannot OOM
 *       the encoder — surfacing a clean {@code 413} business error; and (b) caps inbound reassembly
 *       on both ends so a peer cannot OOM the receiver either.
 * </ul>
 *
 * <p>Only the single-frame-cap is a fixed ~16&nbsp;MB protocol constant, so only {@link
 * #responseOverFrameCapFailsWithoutFragmentation()} and {@link #responseOverFrameCapIsFragmented()}
 * use a large payload. Every other guarantee is proven at KB scale (and the streaming/OOM-safety of
 * the encoder itself is proven separately, allocation-free, in {@code LimitedOutputStreamTest}).
 * Fragmentation and the watermark compose: to allow legitimately large (fragmented) responses while
 * rejecting anything beyond a ceiling, set {@code maxMessageSize} above the frame cap and pair it
 * with {@code mtu}.
 *
 * <p><b>Honest scope:</b> these knobs bound serialization and reassembly. They cannot stop a service
 * from OOMing while building its own result objects before encoding — that requires bounding the
 * query (pagination/streaming) at the service. {@link #streamSucceeds()} shows that shape.
 */
final class RSocketLargePayloadTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final LoopResources LOOP_RESOURCE = LoopResources.create("large-payload-test");

  /** RSocket frame length is a 24-bit field: max single-frame payload is {@code 2^24 - 1}. */
  private static final int MAX_FRAME_LENGTH = 0xFFFFFF; // 16_777_215

  /** Just over the single-frame cap — the smallest payload that needs fragmentation. */
  private static final int OVER_FRAME_CAP = MAX_FRAME_LENGTH + 1;

  private static final int FRAGMENT_MTU = 1024 * 1024; // 1 MiB, for the frame-cap fragmentation test
  private static final int WATERMARK = 8 * 1024; // 8 KiB high-watermark for the KB-scale cases

  private Microservices service;
  private ServiceCall serviceCall;

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
    if (service != null) {
      service.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // single-frame cap (the fixed ~16 MB protocol constant) — the only large-payload cases
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("default: a response within the single-frame cap is delivered")
  void smallResponseSucceeds() {
    final var api = payloadService(0, 0, 0);

    final byte[] payload = api.getPayload(1024).block(TIMEOUT);
    assertNotNull(payload);
    assertEquals(1024, payload.length);
  }

  @Test
  @DisplayName("default: a response over the single-frame cap fails with the RSocket frame error")
  void responseOverFrameCapFailsWithoutFragmentation() {
    final var api = payloadService(0, 0, 0);

    StepVerifier.create(api.getPayload(OVER_FRAME_CAP))
        .expectErrorSatisfies(
            ex -> {
              assertNotNull(ex.getMessage());
              assertTrue(
                  ex.getMessage().contains("too big to be send as a single frame"),
                  "unexpected: " + ex);
              assertTrue(
                  ex.getMessage().contains(String.valueOf(MAX_FRAME_LENGTH)),
                  "should state the 24-bit cap: " + ex);
            })
        .verify(TIMEOUT);
  }

  @Test
  @DisplayName("fragment: a response over the single-frame cap is fragmented and delivered whole")
  void responseOverFrameCapIsFragmented() {
    final var api = payloadService(FRAGMENT_MTU, 0, 0);

    final byte[] payload = api.getPayload(OVER_FRAME_CAP).block(TIMEOUT);
    assertNotNull(payload);
    assertEquals(OVER_FRAME_CAP, payload.length);
  }

  // ---------------------------------------------------------------------------------------------
  // watermark — business error beyond it (KB scale; no fragmentation needed to prove the point)
  // ---------------------------------------------------------------------------------------------

  // Note: these boundary cases use a String response (encoded data = length + 2 JSON quote bytes),
  // not byte[]. A Mono<byte[]> method cannot be used here because ServiceMessageCodec.decodeData
  // decodes a byte[]-typed response before checking isError(), so a byte[] client swallows error
  // responses as raw bytes — a pre-existing scalecube quirk unrelated to the watermark.

  @Test
  @DisplayName("watermark: a response exactly at the watermark is allowed")
  void responseExactlyAtWatermarkSucceeds() {
    final var api = payloadService(0, WATERMARK, 0);

    final String payload = api.getText(WATERMARK - 2).block(TIMEOUT); // encoded data == limit
    assertNotNull(payload);
    assertEquals(WATERMARK - 2, payload.length());
  }

  @Test
  @DisplayName("watermark: one byte over the watermark fails fast with code 413 and the exact limit")
  void responseOneByteOverWatermarkFailsFast() {
    final var api = payloadService(0, WATERMARK, 0);

    StepVerifier.create(api.getText(WATERMARK - 1)) // encoded data == watermark + 1
        .expectErrorSatisfies(
            ex -> {
              final var se = assertInstanceOf(ServiceException.class, ex);
              assertEquals(413, se.errorCode(), "errorCode");
              assertEquals("Message size exceeds limit (" + WATERMARK + " bytes)", se.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  @DisplayName("watermark: a realistic unbounded collection response is aborted mid-encode (413)")
  void oversizedCollectionResponseFailsFast() {
    final var api = payloadService(0, WATERMARK, 0);

    // ~50 KB of JSON vs an 8 KB watermark: the streaming encoder aborts after ~8 KB, never building
    // the whole array. Same shape as an unbounded "fetch everything" query, in miniature.
    StepVerifier.create(api.getItems(50))
        .expectErrorSatisfies(
            ex -> {
              final var se = assertInstanceOf(ServiceException.class, ex);
              assertEquals(413, se.errorCode());
              assertTrue(se.getMessage().contains("exceeds limit"), "unexpected: " + ex);
            })
        .verify(TIMEOUT);
  }

  // ---------------------------------------------------------------------------------------------
  // inbound reassembly cap — protects the receiver from a peer that ignores the watermark
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("inbound cap: a response larger than the client's reassembly cap is rejected")
  void responseExceedingClientInboundCapIsRejected() {
    // RSocket requires the inbound cap to be >= the single-frame cap, so the smallest meaningful cap
    // is the frame cap itself. Server fragments freely with no watermark; the client caps reassembly
    // at exactly the frame cap, and a payload one byte larger is rejected on reassembly.
    final var api = payloadService(FRAGMENT_MTU, 0, MAX_FRAME_LENGTH);

    StepVerifier.create(api.getPayload(OVER_FRAME_CAP)) // one byte over the client's reassembly cap
        .expectError()
        .verify(TIMEOUT);
  }

  // ---------------------------------------------------------------------------------------------
  // request side — the same guard protects the client from building an oversized request
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("request: an oversized request fails fast on the client, before the wire (413)")
  void oversizedRequestFailsFastOnClient() {
    final var api = payloadService(0, 0, WATERMARK);

    StepVerifier.create(api.upload(new byte[WATERMARK + 1]))
        .expectErrorSatisfies(
            ex -> {
              final var se = assertInstanceOf(ServiceException.class, ex);
              assertEquals(413, se.errorCode());
              assertTrue(se.getMessage().contains("exceeds limit"), "unexpected: " + ex);
            })
        .verify(TIMEOUT);
  }

  // ---------------------------------------------------------------------------------------------
  // control — streaming is never a single oversized payload
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("control: a streamed response (one frame per element) is unaffected by the limits")
  void streamSucceeds() {
    final var api = payloadService(0, WATERMARK, 0);

    StepVerifier.create(api.streamItems(200)) // each element well under the watermark
        .expectNextCount(200)
        .expectComplete()
        .verify(TIMEOUT);
  }

  // ---------------------------------------------------------------------------------------------
  // harness
  // ---------------------------------------------------------------------------------------------

  /**
   * Starts a provider and returns a client proxy.
   *
   * @param serverMtu server fragmentation MTU ({@code 0} = off)
   * @param serverMaxMessageSize server high-watermark ({@code 0} = unbounded)
   * @param clientMaxMessageSize client high-watermark / inbound reassembly cap ({@code 0} =
   *     unbounded)
   */
  private PayloadService payloadService(
      int serverMtu, int serverMaxMessageSize, int clientMaxMessageSize) {
    // The two knobs under test live on RSocketServiceTransport — the normal place you configure the
    // transport. Both are opt-in (default 0 = off = legacy behavior):
    //   .mtu(n)            enable fragmentation so responses larger than the 16 MB frame cap can be
    //                      sent in pieces.
    //   .maxMessageSize(n) the high-watermark: a message larger than this fails fast while encoding
    //                      with a 413 (streaming guard, never fully buffered), and inbound reassembly
    //                      is capped at the same size (when n >= the frame cap). Set per node, so it
    //                      must be configured on both the provider (here) and the caller (below).
    service =
        Microservices.start(
            new Context()
                .transport(
                    () ->
                        new RSocketServiceTransport()
                            .mtu(serverMtu)
                            .maxMessageSize(serverMaxMessageSize))
                .services(new PayloadServiceImpl()));

    serviceCall =
        new ServiceCall()
            .transport(
                // Built directly here for an explicit per-call config; the production path is the
                // same RSocketServiceTransport builder as above. The trailing two args are the same
                // knobs at the low-level constructor: (..., mtu, maxMessageSize). mtu=0 (the client
                // only sends tiny requests); maxMessageSize bounds the client's outgoing requests and
                // caps inbound reassembly of responses it receives.
                new RSocketClientTransport(
                    HeadersCodec.DEFAULT_INSTANCE,
                    DataCodec.getAllInstances(),
                    RSocketClientTransportFactory.websocket().apply(LOOP_RESOURCE),
                    null,
                    null,
                    0,
                    clientMaxMessageSize))
            .router(StaticAddressRouter.forService(service.serviceAddress(), "payloads").build());

    return serviceCall.api(PayloadService.class);
  }

  @Service("v1/payloads")
  public interface PayloadService {

    /** Returns a response payload of exactly {@code size} bytes. */
    @ServiceMethod("payload")
    Mono<byte[]> getPayload(Integer size);

    /** Returns a String of {@code length} chars (encoded data = {@code length + 2} bytes). */
    @ServiceMethod("text")
    Mono<String> getText(Integer length);

    /** Echoes the size of the request payload (used to test the request-side guard). */
    @ServiceMethod("upload")
    Mono<Integer> upload(byte[] data);

    /** Unbounded "fetch everything" query — the whole result as a single response payload. */
    @ServiceMethod("items")
    Mono<List<Item>> getItems(Integer count);

    /** Same data, streamed one item per frame. */
    @ServiceMethod("items/stream")
    Flux<Item> streamItems(Integer count);
  }

  public static class PayloadServiceImpl implements PayloadService {

    private static final String DATA = "x".repeat(1024); // ~1 KiB per item

    @Override
    public Mono<byte[]> getPayload(Integer size) {
      return Mono.fromSupplier(() -> new byte[size]);
    }

    @Override
    public Mono<String> getText(Integer length) {
      return Mono.fromSupplier(() -> "x".repeat(length));
    }

    @Override
    public Mono<Integer> upload(byte[] data) {
      return Mono.just(data.length);
    }

    @Override
    public Mono<List<Item>> getItems(Integer count) {
      return Mono.fromSupplier(() -> items(count));
    }

    @Override
    public Flux<Item> streamItems(Integer count) {
      return Flux.fromIterable(items(count));
    }

    private static List<Item> items(int count) {
      final var items = new ArrayList<Item>(count);
      for (int i = 0; i < count; i++) {
        items.add(new Item(i, DATA));
      }
      return items;
    }
  }

  public static class Item {

    private long id;
    private String data;

    public Item() {}

    public Item(long id, String data) {
      this.id = id;
      this.data = data;
    }

    public long id() {
      return id;
    }

    public String data() {
      return data;
    }
  }
}
