package io.scalecube.services.transport.rsocket;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream that aborts encoding the moment the number of bytes written would exceed {@code
 * limit}, throwing {@link MessageTooLargeException} <em>before</em> the bytes reach the underlying
 * stream. This bounds memory during encoding: an oversized message can never be fully materialized
 * into the target buffer, so a runaway response cannot OOM the encoder — it fails fast instead.
 */
final class LimitedOutputStream extends FilterOutputStream {

  private final long limit;
  private long written;

  LimitedOutputStream(OutputStream out, long limit) {
    super(out);
    this.limit = limit;
  }

  @Override
  public void write(int b) throws IOException {
    ensureCapacity(1);
    out.write(b);
    written++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureCapacity(len);
    out.write(b, off, len);
    written += len;
  }

  private void ensureCapacity(int n) {
    if (written + n > limit) {
      throw new MessageTooLargeException("Message size exceeds limit (" + limit + " bytes)");
    }
  }
}
