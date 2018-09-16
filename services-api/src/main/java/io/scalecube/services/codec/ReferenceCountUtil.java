package io.scalecube.services.codec;

import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReferenceCountUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReferenceCountUtil.class);

  private ReferenceCountUtil() {
    // Do not instantiate
  }

  /**
   * Try to release input object iff it's instance is of {@link ReferenceCounted} type and its
   * refCount greater than zero.
   *
   * @return true if msg release taken place
   */
  public static boolean safestRelease(Object msg) {
    try {
      return (msg instanceof ReferenceCounted)
          && ((ReferenceCounted) msg).refCnt() > 0
          && ((ReferenceCounted) msg).release();
    } catch (Throwable t) {
      LOGGER.warn("Failed to release reference counted object: {}", msg, t);
      return false;
    }
  }
}
