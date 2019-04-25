package io.scalecube.services.gateway.rsocket;

import io.rsocket.Payload;
import io.scalecube.services.gateway.AbstractLocalGatewayExtension;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import io.scalecube.services.gateway.clientsdk.rsocket.RSocketClientCodec;
import io.scalecube.services.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import reactor.netty.resources.LoopResources;

class RsocketLocalGatewayExtension extends AbstractLocalGatewayExtension {

  private static final String GATEWAY_ALIAS_NAME = "rsws";

  RsocketLocalGatewayExtension(Object serviceInstance) {
    super(serviceInstance, opts -> new RSocketGateway(opts.id(GATEWAY_ALIAS_NAME)));
  }

  @Override
  protected ClientTransport transport() {
    return new RSocketClientTransport(
        clientSettings(), clientMessageCodec(), LoopResources.create(gatewayAliasName() + "-loop"));
  }

  @Override
  protected ClientCodec<Payload> clientMessageCodec() {
    String contentType = clientSettings().contentType();
    HeadersCodec headersCodec = HeadersCodec.getInstance(contentType);
    DataCodec dataCodec = DataCodec.getInstance(contentType);

    return new RSocketClientCodec(headersCodec, dataCodec);
  }

  @Override
  protected String gatewayAliasName() {
    return GATEWAY_ALIAS_NAME;
  }
}
