package io.scalecube.services.registry.api;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.Microservices.Builder;
import io.scalecube.services.registry.api.ServicesConfig.Builder.ServiceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServicesConfig {

  private static final ServicesConfig EMPTY_SERVICES_CONFIG = new ServicesConfig();

  private List<ServiceConfig> servicesConfig = new ArrayList<>();

  public void setServiceTags(List<ServiceConfig> serviceTags) {
    this.servicesConfig = serviceTags;
  }

  ServicesConfig() {}

  ServicesConfig(List<ServiceConfig> unmodifiableList) {
    this.servicesConfig = unmodifiableList;
  }

  public List<ServiceConfig> services() {
    return this.servicesConfig;
  }
  
  public static ServicesConfig empty() {
    return EMPTY_SERVICES_CONFIG;
  }
}
