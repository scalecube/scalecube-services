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

import io.scalecube.services.annotations.Inject;

import java.util.concurrent.CompletableFuture;

public class CoarseGrainedServiceImpl implements CoarseGrainedService {

  private GreetingService greetingService;

  public CoarseGrainedServiceImpl() {
    // default;
  };

  @Inject
  public CoarseGrainedServiceImpl(GreetingService greetingService) {
    this.greetingService = greetingService;
  }

  public void setGreetingServiceProxy(GreetingService subService) {
    this.greetingService = subService;
  }

  @Override
  public CompletableFuture<String> callGreeting(String name) {
    return this.greetingService.greeting(name);
  }
}
