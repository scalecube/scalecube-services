package io.scalecube.services.transport.protostuff;

import io.netty.util.Recycler;
import io.protostuff.LinkedBuffer;
import java.util.Objects;

/**
 * Facility class for {@link LinkedBuffer}. Based on idea of object pooling (done vian {@link
 * Recycler}). Typical usage:
 *
 * <pre>
 *     RecycleableLinkedBuffer rlb = new RecycleableLinkedBuffer(bufferSize, maxCapacity)
 *     LinkedBuffer lb = rlb.get();
 *     ...
 *     rlb.release();
 * </pre>
 */
final class RecyclableLinkedBuffer implements AutoCloseable {
  public static final int DEFAULT_BUFFER_SIZE = 512;
  public static final int DEFAULT_MAX_CAPACITY = 256;

  private LinkedBuffer buffer;
  private Recycler.Handle handle;
  private final Recycler<RecyclableLinkedBuffer> recycler;

  public RecyclableLinkedBuffer() {
    this(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_CAPACITY);
  }

  /**
   * Creates new {@link RecyclableLinkedBuffer}.
   *
   * @param bufferSize {@link LinkedBuffer}'s buffer size.
   * @param maxCapacity {@link Recycler}'s.
   */
  public RecyclableLinkedBuffer(final int bufferSize, int maxCapacity) {
    this.recycler =
        new Recycler<RecyclableLinkedBuffer>(maxCapacity) {
          @Override
          protected RecyclableLinkedBuffer newObject(Handle handle) {
            RecyclableLinkedBuffer wrapper = new RecyclableLinkedBuffer();
            wrapper.buffer = LinkedBuffer.allocate(bufferSize);
            wrapper.handle = handle;
            return wrapper;
          }
        };
  }

  public LinkedBuffer buffer() {
    return Objects.requireNonNull(buffer, "Call LinkedBufferWrapper.get() first");
  }

  public RecyclableLinkedBuffer get() {
    return recycler.get();
  }

  public void release() {
    Objects.requireNonNull(buffer, "Call LinkedBufferWrapper.get() first");
    buffer.clear();
    recycler.recycle(this, handle);
  }

  @Override
  public void close() {
    release();
  }
}
