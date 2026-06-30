package io.scalecube.services.transport.rsocket;

/** RSocket protocol constants shared across the transport. */
final class RSocketConstants {

  /**
   * RSocket single-frame cap: the frame-length header is 24 bits, so no single (non-fragmented)
   * frame can exceed {@code 2^24 - 1} bytes. {@code maxInboundPayloadSize} must be at least this (a
   * reassembly buffer must hold one full frame).
   */
  static final int MAX_FRAME_LENGTH = 0xFFFFFF; // 16_777_215

  /** RSocket minimum fragmentation MTU. */
  static final int MIN_MTU = 64;

  private RSocketConstants() {
    // do not instantiate
  }
}
