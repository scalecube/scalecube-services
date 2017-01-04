/*
 * Copyright 2017 nuwan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.scalecube.examples.services;

import io.scalecube.services.annotations.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author nuwan
 */
public class TicketServiceImpl implements TicketService {

  AtomicInteger maxTicketCount = new AtomicInteger(10);

  @Inject
  public TicketServiceImpl(TickerServiceConfig config) {
    this.maxTicketCount = new AtomicInteger(config.getMaxTicketCount());
  }
  
  @Override
  public CompletableFuture<Boolean> reserve(int ticketCount) {
    return CompletableFuture.supplyAsync(() -> maxTicketCount.addAndGet(-ticketCount) >= 0);
  }

}

class TickerServiceConfig {

  int maxTicketCount;

  public TickerServiceConfig(int maxTicketCount) {
    this.maxTicketCount = maxTicketCount;
  }

  public int getMaxTicketCount() {
    return maxTicketCount;
  }

}
