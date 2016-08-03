package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkState;

import io.netty.util.Recycler;
import io.protostuff.LinkedBuffer;

/**
 * Facility class for {@link io.protostuff.LinkedBuffer}. Based on idea of object pooling (done vian
 * {@link io.netty.util.Recycler}).
 * <p/>
 * Typical usage:
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
   * @param bufferSize {@link io.protostuff.LinkedBuffer}'s buffer size.
   * @param maxCapacity {@link io.netty.util.Recycler}'s.
   */
  public RecyclableLinkedBuffer(final int bufferSize, int maxCapacity) {
    this.recycler = new Recycler<RecyclableLinkedBuffer>(maxCapacity) {
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
    checkState(buffer != null, "Call LinkedBufferWrapper.get() first");
    return buffer;
  }

  public RecyclableLinkedBuffer get() {
    return recycler.get();
  }

  public void release() {
    checkState(buffer != null, "Call LinkedBufferWrapper.get() first");
    buffer.clear();
    recycler.recycle(this, handle);
  }

  @Override
  public void close() {
    release();
  }
}
