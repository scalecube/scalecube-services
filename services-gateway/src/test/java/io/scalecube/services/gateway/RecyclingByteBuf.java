package io.scalecube.services.gateway;

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Heap buffer that mimics {@code PooledByteBuf} recycling: on deallocation the instance is handed
 * to a new owner (refCnt reset to 1, memory kept), as {@code PooledByteBuf.reuse()} does. A stale
 * {@code release()} therefore lands on that owner instead of on a dead buffer, and is counted
 * rather than silently skipped by {@code refCnt() > 0} guards.
 */
public final class RecyclingByteBuf extends UnpooledHeapByteBuf {

  private final AtomicInteger deallocations = new AtomicInteger();

  public RecyclingByteBuf(int capacity) {
    super(UnpooledByteBufAllocator.DEFAULT, capacity, capacity);
  }

  @Override
  protected void deallocate() {
    deallocations.incrementAndGet();
    resetRefCnt();
  }

  public int deallocations() {
    return deallocations.get();
  }
}
