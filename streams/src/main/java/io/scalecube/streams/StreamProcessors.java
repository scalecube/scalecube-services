package io.scalecube.streams;

import io.scalecube.transport.Address;

public final class StreamProcessors {

  private StreamProcessors() {
    // Do not instantiate
  }

  /**
   * Factory method for creating a factory for stream processors with client side semantics. Returned
   * {@link ClientStreamProcessors} object is immutable.
   *
   * @return factory object for client side {@link StreamProcessor} objects
   * @see ClientStreamProcessors#create(Address)
   */
  public static ClientStreamProcessors newClient() {
    return ClientStreamProcessors.newClientStreamProcessors();
  }

  /**
   * Factory method for creating a factory for stream processors with server side semantics. Returned
   * {@link ServerStreamProcessors} object is immutable.
   *
   * @return factory object for server side {@link StreamProcessor} objects
   * @see ServerStreamProcessors#bind()
   * @see ServerStreamProcessors#bindAwait()
   */
  public static ServerStreamProcessors newServer() {
    return ServerStreamProcessors.newServerStreamProcessors();
  }
}
