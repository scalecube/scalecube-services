package io.scalecube.services.gateway.http;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

public interface HttpGatewayMessageHandler {

  HttpGatewayMessageHandler DEFAULT_INSTANCE = new HttpGatewayMessageHandler() {};

  default void onRequest(HttpServerRequest request, ByteBuf content, ServiceMessage message) {}

  default void onResponse(HttpServerResponse response, ByteBuf content, ServiceMessage message) {}

  default void onError(HttpServerResponse response, ByteBuf content, ServiceMessage message) {}
}
