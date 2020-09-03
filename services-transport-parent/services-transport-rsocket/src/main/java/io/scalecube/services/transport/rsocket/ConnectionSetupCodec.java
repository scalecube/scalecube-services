package io.scalecube.services.transport.rsocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ConnectionSetupCodec {

  ConnectionSetupCodec DEFAULT_INSTANCE = new JdkConnectionSetupCodec();

  void encode(OutputStream stream, ConnectionSetup value) throws IOException;

  ConnectionSetup decode(InputStream stream) throws IOException;
}
