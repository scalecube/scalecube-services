package io.scalecube.services.transport.rsocket;

import io.scalecube.net.Address;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public final class ConnectionSetup implements Externalizable {

  public static final ConnectionSetup NULL_INSTANCE = ConnectionSetup.builder().build();

  private static final long serialVersionUID = 1L;

  private String id;
  private Address address;
  private Set<String> contentTypes;
  private Map<String, String> tags;
  private String headersContentType;
  private Map<String, String> credentials;

  /**
   * Constructor for de/serialization purpose.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  @Deprecated
  public ConnectionSetup() {}

  private ConnectionSetup(Builder builder) {
    this.id = builder.id;
    this.address = builder.address;
    this.contentTypes = Collections.unmodifiableSet(new HashSet<>(builder.contentTypes));
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.headersContentType = builder.headersContentType;
    this.credentials = Collections.unmodifiableMap(new HashMap<>(builder.credentials));
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return id;
  }

  public Address address() {
    return address;
  }

  public Set<String> contentTypes() {
    return contentTypes;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public String headersContentType() {
    return headersContentType;
  }

  public Map<String, String> credentials() {
    return credentials;
  }

  public static final class Builder {

    private String id = "none";
    private Address address = Address.NULL_ADDRESS;
    private Set<String> contentTypes = Collections.emptySet();
    private Map<String, String> tags = Collections.emptyMap();
    private String headersContentType = "application/octet-stream";
    private Map<String, String> credentials = Collections.emptyMap();

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    public Builder contentTypes(Set<String> contentTypes) {
      this.contentTypes = contentTypes;
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder headersContentType(String headersContentType) {
      this.headersContentType = headersContentType;
      return this;
    }

    public Builder credentials(Map<String, String> credentials) {
      this.credentials = credentials;
      return this;
    }

    public ConnectionSetup build() {
      return new ConnectionSetup(this);
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // id
    out.writeUTF(id);

    // address
    out.writeUTF(address.toString());

    // contentTypes
    out.writeInt(contentTypes.size());
    for (String contentType : contentTypes) {
      out.writeUTF(contentType);
    }

    // tags
    out.writeInt(tags.size());
    for (Entry<String, String> entry : tags.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeObject(entry.getValue()); // value is nullable
    }

    // headersContentType
    out.writeUTF(headersContentType);

    // credentials
    out.writeInt(credentials.size());
    for (Entry<String, String> entry : credentials.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeObject(entry.getValue()); // value is nullable
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // id
    id = in.readUTF();

    // address
    address = Address.from(in.readUTF());

    // contentTypes
    int contentTypesSize = in.readInt();
    Set<String> contentTypes = new HashSet<>(contentTypesSize);
    for (int i = 0; i < contentTypesSize; i++) {
      contentTypes.add(in.readUTF());
    }
    this.contentTypes = Collections.unmodifiableSet(contentTypes);

    // tags
    int tagsSize = in.readInt();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      String key = in.readUTF();
      String value = (String) in.readObject(); // value is nullable
      tags.put(key, value);
    }
    this.tags = Collections.unmodifiableMap(tags);

    // headersContentType
    this.headersContentType = in.readUTF();

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
