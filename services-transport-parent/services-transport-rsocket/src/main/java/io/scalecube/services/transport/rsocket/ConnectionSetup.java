package io.scalecube.services.transport.rsocket;

import io.scalecube.utils.MaskUtil;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;

public final class ConnectionSetup implements Externalizable {

  private static final long serialVersionUID = 1L;

  private Map<String, String> credentials = Collections.emptyMap();

  /**
   * Constructor for de/serialization purpose.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  @Deprecated
  public ConnectionSetup() {}

  /**
   * Constructor.
   *
   * @param credentials credentials (not null)
   */
  public ConnectionSetup(Map<String, String> credentials) {
    this.credentials =
        Collections.unmodifiableMap(
            new HashMap<>(Objects.requireNonNull(credentials, "ConnectionSetup.credentials")));
  }

  public Map<String, String> credentials() {
    return credentials;
  }

  public boolean hasCredentials() {
    return !credentials.isEmpty();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ConnectionSetup.class.getSimpleName() + "[", "]")
        .add("credentials=" + MaskUtil.mask(credentials))
        .toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // credentials
    out.writeInt(credentials.size());
    for (Entry<String, String> entry : credentials.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeObject(entry.getValue()); // value is nullable
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // credentials
    int credsSize = in.readInt();
    Map<String, String> creds = new HashMap<>(credsSize);
    for (int i = 0; i < credsSize; i++) {
      String key = in.readUTF();
      String value = (String) in.readObject(); // value is nullable
      creds.put(key, value);
    }
    this.credentials = Collections.unmodifiableMap(creds);
  }
}
