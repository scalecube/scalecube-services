package io.scalecube.services.transport.rsocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class JdkConnectionSetupCodec implements ConnectionSetupCodec {

  @Override
  public void encode(OutputStream stream, ConnectionSetup value) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
      oos.writeObject(value);
      oos.flush();
    }
  }

  @Override
  public ConnectionSetup decode(InputStream stream) throws IOException {
    try (ObjectInputStream is = new ObjectInputStream(stream)) {
      return (ConnectionSetup) is.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}
