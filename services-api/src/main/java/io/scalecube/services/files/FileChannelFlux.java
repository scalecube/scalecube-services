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
    return createFrom(filePath, DEFAULT_CHUNK_SIZE);
  }

  public static Flux<byte[]> createFrom(Path filePath, int chunkSize) {
    return Flux.using(
        () -> FileChannel.open(filePath),
        channel -> {
          final var chunkBuffer = ByteBuffer.allocate(chunkSize);
          return Flux.generate(
              () -> channel,
              (fc, sink) -> {
                try {
                  chunkBuffer.clear();
                  int read = fc.read(chunkBuffer);
                  if (read == -1) {
                    sink.complete();
                    return fc;
                  }
                  if (read > 0) {
                    chunkBuffer.flip();
                    byte[] bytes = new byte[chunkBuffer.remaining()];
                    chunkBuffer.get(bytes);
                    sink.next(bytes);
                  }
                  return fc;
                } catch (IOException e) {
                  sink.error(e);
                  return fc;
                }
              });
        },
        channel -> {
          try {
            channel.close();
          } catch (IOException ex) {
            // no-op
          }
        });
  }
}
