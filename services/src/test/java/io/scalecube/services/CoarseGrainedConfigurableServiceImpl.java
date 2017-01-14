/*
 * Copyright 2017 nuwan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.scalecube.services;

import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.ServiceProxy;
import io.scalecube.services.routing.RoundRobinServiceRouter;

import java.util.concurrent.CompletableFuture;

public class CoarseGrainedConfigurableServiceImpl implements CoarseGrainedService {

  private GreetingService greetingService;
  @Inject
  private Configuration configuration;
  
  public CoarseGrainedConfigurableServiceImpl() {
    // default;
  };

  @Inject
  public CoarseGrainedConfigurableServiceImpl(@ServiceProxy(router = RoundRobinServiceRouter.class,timeout = 10) GreetingService greetingService,Configuration configuration) {
    this.greetingService = greetingService;
    this.configuration = configuration;
  }

  public void setGreetingServiceProxy(GreetingService subService) {
    this.greetingService = subService;
  }

  @Override
  public CompletableFuture<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }
  
  
  public static final class Configuration
  {
    String language;

    public Configuration(String language) {
      this.language = language;
    }

    public String getLanguage() {
      return language;
    }
    
  }
}
