package io.scalecube.services.gateway;

import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import reactor.util.context.Context;

public class TestGatewaySessionHandler implements GatewaySessionHandler {

  public final CountDownLatch msgLatch = new CountDownLatch(1);
  public final CountDownLatch connLatch = new CountDownLatch(1);
  public final CountDownLatch disconnLatch = new CountDownLatch(1);
  private final AtomicReference<GatewaySession> lastSession = new AtomicReference<>();

  @Override
  public ServiceMessage mapMessage(GatewaySession s, ServiceMessage req, Context context) {
    msgLatch.countDown();
    return req;
  }

  @Override
  public void onSessionOpen(GatewaySession s) {
    connLatch.countDown();
    lastSession.set(s);
  }

  @Override
  public void onSessionClose(GatewaySession s) {
    disconnLatch.countDown();
  }

  public GatewaySession lastSession() {
    return lastSession.get();
  }
}
