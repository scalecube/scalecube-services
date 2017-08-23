package io.scalecube.services;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Splitter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class used to register service with tags as metadata in the scalecube cluster. parsing from service info to
 * json and back.
 */
public class ServiceInfo {

  private static final String EQ = "=";

  private static final String KEY_VALUE_SEPERATOR = ":eq:";

  private static final String TAG_SEPERATOR = "|tag|";

  private static final String TAGS_SPERATOR = ":tags:";

  private String serviceName;

  private Map<String, String> tags;

  public ServiceInfo(String serviceName, Map<String, String> tags) {
    this.serviceName = serviceName;
    this.tags = tags;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public String getServiceName() {
    return serviceName;
  }

  /**
   * Gets a service info instance by a given metadata string SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|.
   * 
   * @param metadata string as follow : SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|.
   * @return initialized service info.
   */
  public static ServiceInfo from(String metadata) {
    checkNotNull(metadata);
    int index = metadata.indexOf(TAGS_SPERATOR, 0);
    String name = metadata.substring(0, index);
    String tagsAsString = metadata.substring(index + TAGS_SPERATOR.length());
    List<String> list = Splitter.on(TAG_SEPERATOR).splitToList(tagsAsString);

    Map<String, String> tags = new HashMap<>();
    for (String element : list) {
      if (element.length() > 0) {
        List<String> kv = Splitter.on(EQ).splitToList(element);
        tags.put(kv.get(0).replaceAll(KEY_VALUE_SEPERATOR, EQ), kv.get(1).replaceAll(KEY_VALUE_SEPERATOR, EQ));
      }
    }
    return new ServiceInfo(name, tags);
  }

  /**
   * returns a service info as metadata string: SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|.
   * 
   * @return initialized service info - SERVICE_NAME:tags:key1=value1|tag|key2=value2|tag|..
   */
  public String toMetadata() {
    StringBuilder sb = new StringBuilder(serviceName);
    sb.append(TAGS_SPERATOR);
    tags.entrySet().stream().forEach(kv -> {
      sb.append(kv.getKey().replaceAll(EQ, KEY_VALUE_SEPERATOR));
      sb.append(EQ);
      sb.append(kv.getValue().replaceAll(EQ, KEY_VALUE_SEPERATOR));
      sb.append(TAG_SEPERATOR);
    });

    return sb.toString();
  }
}
