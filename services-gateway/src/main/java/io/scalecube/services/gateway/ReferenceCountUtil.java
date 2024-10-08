package io.scalecube.services.gateway;

import io.netty.util.ReferenceCounted;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

public final class ReferenceCountUtil {

  private static final Logger LOGGER = System.getLogger(ReferenceCountUtil.class.getName());

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
      LOGGER.log(
          Level.WARNING,
          "Failed to release reference counted object: {0}, cause: {1}",
          msg,
          t.toString());
      return false;
    }
  }
}
