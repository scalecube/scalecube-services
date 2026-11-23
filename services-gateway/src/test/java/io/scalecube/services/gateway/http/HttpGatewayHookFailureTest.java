package io.scalecube.services.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.ServiceMessage;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.netty.http.server.HttpServerRequest;

/** A failing message handler hook must not leak the request data. */
class HttpGatewayHookFailureTest {

  private Microservices gateway;

  @AfterEach
  void afterEach() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void failingOnRequestReleasesRequestData() throws Exception {
    final AtomicReference<ByteBuf> data = new AtomicReference<>();
    gateway =
        Microservices.start(
            new Context()
                .gateway(
                    () ->
                        HttpGateway.builder()
                            .id("HTTP")
                            .messageHandler(
                                new HttpGatewayMessageHandler() {
                                  @Override
                                  public void onRequest(
                                      HttpServerRequest request,
                                      ByteBuf content,
                                      ServiceMessage message) {
                                    data.set(content);
                                    throw new IllegalStateException("onRequest failed");
                                  }
                                })
                            .build()));

    final var request =
        new Request.Builder()
            .url("http://localhost:" + gateway.gateway("HTTP").address().port() + "/v1/api/hello")
            .post(RequestBody.create("\"hello\"", MediaType.get("application/json")))
            .build();
    try (Response response = new OkHttpClient().newCall(request).execute()) {
      assertEquals(500, response.code(), "response.code");
    }

    assertNotNull(data.get(), "onRequest content");
    assertEquals(0, data.get().refCnt(), "request data refCnt");
  }
}
