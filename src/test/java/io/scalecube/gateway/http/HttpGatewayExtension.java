package io.scalecube.gateway.http;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.AbstractGatewayExtension;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.http.HttpClientCodec;
import io.scalecube.gateway.clientsdk.http.HttpClientTransport;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.gateway.GatewayConfig;
import reactor.ipc.netty.resources.LoopResources;

class HttpGatewayExtension extends AbstractGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "http";

  HttpGatewayExtension(Object serviceInstance) {
    super(serviceInstance, GatewayConfig.builder(GATEWAY_ALIAS_NAME, HttpGateway.class).build());
  }

  @Override
  protected ClientTransport transport() {
    return new HttpClientTransport(
        clientSettings(), clientMessageCodec(), LoopResources.create(gatewayAliasName() + "-loop"));
  }

  @Override
  protected ClientCodec<ByteBuf> clientMessageCodec() {
    return new HttpClientCodec(DataCodec.getInstance(clientSettings().contentType()));
  }

  @Override
  protected String gatewayAliasName() {
    return GATEWAY_ALIAS_NAME;
  }
}
