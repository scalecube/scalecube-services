package io.scalecube.ipc.netty;

import io.scalecube.ipc.ChannelContext;

import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.ReferenceCounted;

import java.util.NoSuchElementException;
import java.util.Optional;

public final class ChannelSupport {

  public static final AttributeKey<ChannelContext> CHANNEL_CTX_ATTR_KEY = AttributeKey.valueOf("channelContext");

  public static final Object CHANNEL_CTX_CREATED_EVENT = new Object(); // custom event object

  private ChannelSupport() {
    // Do not instantiate
  }

  /**
   * Try to call {@link ReferenceCounted#release()} on object. Does not call release() if reference count already zero.
   * If object is null or doesn't implement {@link ReferenceCounted} this method does nothing.
   */
  public static boolean releaseRefCount(Object obj) {
    return obj instanceof ReferenceCounted && ((ReferenceCounted) obj).refCnt() > 0
        && ((ReferenceCounted) obj).release();
  }

  public static ChannelContext getChannelContextIfExist(AttributeMap attributeMap) {
    return attributeMap.attr(CHANNEL_CTX_ATTR_KEY).get();
  }

  public static ChannelContext getChannelContextOrThrow(AttributeMap attributeMap) {
    return Optional.ofNullable(getChannelContextIfExist(attributeMap))
        .orElseThrow(() -> new NoSuchElementException("Can't find channel context at attribute map: " + attributeMap));
  }

  public static void closeChannelContextIfExist(AttributeMap attributeMap) {
    Optional.ofNullable(getChannelContextIfExist(attributeMap)).ifPresent(ChannelContext::close);
  }
}
