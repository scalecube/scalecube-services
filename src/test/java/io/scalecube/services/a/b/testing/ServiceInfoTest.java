package io.scalecube.services.a.b.testing;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.ServiceInfo;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class ServiceInfoTest extends BaseTest {

  @Test
  public void test_service_info() {

    Map<String, String> tags = new HashMap<>();
    tags.put("key1", "value1");
    tags.put("key2", "value2");

    ServiceInfo info = new ServiceInfo("service1",null, tags);

    String metadata = info.toMetadata();

    ServiceInfo info2 = ServiceInfo.from(metadata);
    assertTrue(info2.getServiceName().equals("service1"));
    assertTrue(info2.getTags().get("key1").equals("value1"));
    assertTrue(info2.getTags().get("key2").equals("value2"));
  }

  @Test
  public void test_service_info_no_tags() {

    Map<String, String> tags = new HashMap<>();

    ServiceInfo info = new ServiceInfo("service1",null, tags);

    String metadata = info.toMetadata();

    ServiceInfo info2 = ServiceInfo.from(metadata);
    assertTrue(info2.getServiceName().equals("service1"));

  }

  @Test
  public void test_service_info_one_tag() {

    Map<String, String> tags = new HashMap<>();

    ServiceInfo info = new ServiceInfo("service1",null, tags);
    tags.put("key1", "value1");

    String metadata = info.toMetadata();

    ServiceInfo info2 = ServiceInfo.from(metadata);
    assertTrue(info2.getServiceName().equals("service1"));
    assertTrue(info2.getTags().get("key1").equals("value1"));
  }

  @Test
  public void test_service_info_one_tag_with_equals() {

    Map<String, String> tags = new HashMap<>();

    ServiceInfo info = new ServiceInfo("service1",null, tags);
    tags.put("key1", "value1=1");
    String metadata = info.toMetadata();

    ServiceInfo info2 = ServiceInfo.from(metadata);
    assertTrue(info2.getServiceName().equals("service1"));
    assertTrue(info2.getTags().get("key1").equals("value1=1"));
  }

}
