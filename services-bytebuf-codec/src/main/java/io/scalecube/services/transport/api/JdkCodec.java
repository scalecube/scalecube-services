package io.scalecube.services.transport.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;

public class JdkCodec implements DataCodec {

  @Override
  public String contentType() {
    return "application/octet-stream";
  }

  @Override
  public void encode(OutputStream stream, Object value) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
      oos.writeObject(value);
      oos.flush();
    }
  }

  @Override
  public Object decode(InputStream stream, Type type) throws IOException {
    try (ObjectInputStream is = new ObjectInputStream(stream)) {
      return is.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}
