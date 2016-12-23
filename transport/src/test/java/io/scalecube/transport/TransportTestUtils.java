package io.scalecube.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Anton Kharenko
 */
public final class TransportTestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportTestUtils.class);

  public static int CONNECT_TIMEOUT = 100;
  public static int DEFAULT_PORT = 5800;

  private TransportTestUtils() {
    // Do not instantiate
  }

  public static Transport createTransport() {
    TransportConfig config = TransportConfig.builder()
        .connectTimeout(CONNECT_TIMEOUT)
        .useNetworkEmulator(true)
        .port(DEFAULT_PORT)
        .build();
    return Transport.bindAwait(config);
  }

  public static void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      CompletableFuture<Void> close = new CompletableFuture<>();
      transport.stop(close);
      try {
        close.get(1, TimeUnit.SECONDS);
      } catch (Exception ignore) {
        LOGGER.warn("Failed to await transport termination");
      }
    }
  }

  public static void send(final Transport from, final Address to, final Message msg) {
    final CompletableFuture<Void> promise = new CompletableFuture<>();
    promise.thenAccept(aVoid -> {
      if (promise.isDone()) {
        try {
          promise.get();
        } catch (Exception e) {
          LOGGER.error("Failed to send {} to {} from transport: {}, cause: {}", msg, to, from, e.getCause());
        }
      }
    });
    from.send(to, msg, promise);
  }
}
