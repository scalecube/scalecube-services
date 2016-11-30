package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.ServiceConfig.Builder.ServiceContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;

public class ServiceTagsTest {

  @Test
  public void create_and_validate_tags() throws IOException {

    ServiceConfig config = ServiceConfig.builder()
        .service("service1").tag("key1", "value1").tag("key2", "value2").add()
        .service("service2").tag("key3", "value3").tag("key4", "value4").add()
        .build();

    ServiceContext service = config.services().get(0);

    // TEST SERVICE 1
    assertTrue(service.getService().equals("service1"));

    // TEST SERVICE 1 KEYS
    assertTrue(service.getTags()[0].getKey().equals("key1"));
    assertTrue(service.getTags()[1].getKey().equals("key2"));

    // TEST SERVICE 1 VALUES
    assertTrue(service.getTags()[0].getValue().equals("value1"));
    assertTrue(service.getTags()[1].getValue().equals("value2"));


    // TEST SERVICE 2
    service = config.services().get(1);
    assertTrue(service.getService().equals("service2"));

    // TEST SERVICE 2 KEYS
    assertTrue(service.getTags()[0].getKey().equals("key3"));
    assertTrue(service.getTags()[1].getKey().equals("key4"));

    // TEST SERVICE 2 VALUES
    assertTrue(service.getTags()[0].getValue().equals("value3"));
    assertTrue(service.getTags()[1].getValue().equals("value4"));

  }

  @Test
  public void create_and_serialize_deserialize_tags() throws JsonProcessingException {
    ServiceConfig config = ServiceConfig.builder()
        .service("service1").tag("key1", "value1").tag("key2", "value2").add()
        .service("service2").tag("key3", "value3").tag("key4", "value4").add()
        .build();

    ObjectMapper mapper = new ObjectMapper();
    String value = mapper.writeValueAsString(config.services().get(0).getTags());
    assertTrue("[{\"key\":\"key1\",\"value\":\"value1\"},{\"key\":\"key2\",\"value\":\"value2\"}]".equals(value));

  }
}
