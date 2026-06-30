package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit spec for {@link LimitedOutputStream} — the streaming guard that makes the maximum-message-size
 * watermark <em>OOM-safe</em>.
 *
 * <p>The key property is that the check happens <b>on the fly</b>, per write, <em>before</em> the
 * bytes reach the underlying buffer: as soon as the running total would cross the limit the stream
 * throws {@link MessageTooLargeException}, so an oversized message is aborted mid-encode and is
 * never fully materialized. These tests pin exactly that — including that no byte past the limit is
 * ever handed to the delegate.
 */
class LimitedOutputStreamTest {

  @Test
  @DisplayName("writing exactly the limit is allowed and reaches the delegate")
  void allowsExactlyTheLimit() throws IOException {
    final var sink = new ByteArrayOutputStream();
    try (var out = new LimitedOutputStream(sink, 4)) {
      out.write(new byte[] {1, 2, 3, 4}); // exactly the limit
    }
    assertEquals(4, sink.size());
  }

  @Test
  @DisplayName("a bulk write that would overflow throws and writes nothing")
  void bulkWriteThatOverflowsThrowsBeforeWriting() {
    final var sink = new ByteArrayOutputStream();
    final var out = new LimitedOutputStream(sink, 4);

    final var ex =
        assertThrows(
            MessageTooLargeException.class, () -> out.write(new byte[] {1, 2, 3, 4, 5}));

    assertEquals(0, sink.size(), "no bytes may reach the delegate when the chunk would overflow");
    assertEquals(413, ex.errorCode(), "errorCode");
    assertTrue(ex.getMessage().contains("exceeds limit (4 bytes)"), ex.getMessage());
  }

  @Test
  @DisplayName("single-byte writes abort on the very byte that crosses the limit")
  void singleByteWriteAbortsOnCrossing() throws IOException {
    final var sink = new ByteArrayOutputStream();
    final var out = new LimitedOutputStream(sink, 3);

    out.write(1);
    out.write(2);
    out.write(3); // fills the limit exactly

    assertThrows(MessageTooLargeException.class, () -> out.write(4)); // the crossing byte
    assertEquals(3, sink.size(), "only limit bytes reached the delegate");
  }

  @Test
  @DisplayName("streaming a would-be huge payload never buffers more than the limit (OOM-safe)")
  void neverMaterializesMoreThanTheLimit() {
    // Simulate a serializer streaming an enormous object in chunks. Unbounded this would buffer
    // 8 GiB; the limiter must stop it after at most `limit` bytes.
    final int limit = 64 * 1024; // 64 KiB
    final var sink = new ByteArrayOutputStream();
    final var out = new LimitedOutputStream(sink, limit);
    final var chunk = new byte[8 * 1024]; // 8 KiB chunks

    assertThrows(
        MessageTooLargeException.class,
        () -> {
          for (int i = 0; i < 1024 * 1024; i++) { // would be 8 GiB if unbounded
            out.write(chunk);
          }
        });

    assertTrue(sink.size() <= limit, "buffered bytes must never exceed the limit, was " + sink.size());
  }

  @Test
  @DisplayName("limit of 0 via this stream means 'no bytes allowed' (callers pass >0 to enable)")
  void zeroLimitRejectsAnyWrite() {
    // Note: the transport only constructs a LimitedOutputStream when maxMessageSize > 0; this just
    // documents the boundary behavior of the stream itself.
    final var out = new LimitedOutputStream(new ByteArrayOutputStream(), 0);
    assertThrows(MessageTooLargeException.class, () -> out.write(1));
  }
}
