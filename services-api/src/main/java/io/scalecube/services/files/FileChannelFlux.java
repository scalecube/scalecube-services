package io.scalecube.services.files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import reactor.core.publisher.Flux;

public class FileChannelFlux {

  private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

  private FileChannelFlux() {
    // Do not instantiate
  }

  public static Flux<byte[]> createFrom(Path filePath) {
    return createFrom(filePath, ByteBuffer.allocate(DEFAULT_CHUNK_SIZE));
  }

  public static Flux<byte[]> createFrom(Path filePath, ByteBuffer chunkBuffer) {
    return Flux.generate(
        () -> FileChannel.open(filePath),
        (channel, sink) -> {
          try {
            int read;
            chunkBuffer.clear();
            do {
              read = channel.read(chunkBuffer);
            } while (read == 0);

            chunkBuffer.flip();
            if (chunkBuffer.remaining() > 0) {
              final var bytes = new byte[chunkBuffer.remaining()];
              chunkBuffer.get(bytes);
              sink.next(bytes);
            }

            if (read == -1) {
              sink.complete();
            }
          } catch (IOException e) {
            sink.error(e);
          }
          return channel;
        },
        channel -> {
          try {
            channel.close();
          } catch (Exception ex) {
            // no-op
          }
        });
  }
}
