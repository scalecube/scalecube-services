package io.scalecube.services.auth;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import reactor.core.Exceptions;

public class CredentialsCodec {

  private CredentialsCodec() {
    // Do not instantiate
  }

  /**
   * Encodes the given credentials to the given stream.
   *
   * @param stream stream
   * @param credentials credentials
   */
  public static void encode(OutputStream stream, Map<String, String> credentials) {
    if (credentials == null) {
      return;
    }
    Objects.requireNonNull(stream, "output stream");
    try (ObjectOutputStream out = new ObjectOutputStream(stream)) {
      // credentials
      out.writeInt(credentials.size());
      for (Entry<String, String> entry : credentials.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeObject(entry.getValue()); // value is nullable
      }

      out.flush();
    } catch (Throwable th) {
      throw Exceptions.propagate(th);
    }
  }

  /**
   * Encodes the given credentials to a byte array.
   *
   * @param credentials credentials
   * @return byte array representation of credentials
   */
  public static byte[] toByteArray(Map<String, String> credentials) {
    if (credentials == null || credentials.isEmpty()) {
      return new byte[0];
    }
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    encode(output, credentials);
    return output.toByteArray();
  }

  /**
   * Decodes the given stream to credentials as {@code Map<String, String>}.
   *
   * @return credentials
   */
  public static Map<String, String> decode(InputStream stream) {
    Objects.requireNonNull(stream, "input stream");
    try (ObjectInputStream in = new ObjectInputStream(stream)) {
      // credentials
      int credentialsSize = in.readInt();
      Map<String, String> credentials = new HashMap<>(credentialsSize);
      for (int i = 0; i < credentialsSize; i++) {
        String key = in.readUTF();
        String value = (String) in.readObject(); // value is nullable
        credentials.put(key, value);
      }
      return Collections.unmodifiableMap(credentials);
    } catch (Throwable th) {
      throw Exceptions.propagate(th);
    }
  }

  /**
   * Decodes the given byte array to credentials as {@code Map<String, String>}.
   *
   * @return credentials
   */
  public static Map<String, String> decode(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return Collections.emptyMap();
    }
    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    return decode(input);
  }
}
