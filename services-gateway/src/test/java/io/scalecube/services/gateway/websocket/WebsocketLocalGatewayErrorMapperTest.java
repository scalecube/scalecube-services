package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.TestUtils.TIMEOUT;
import static io.scalecube.services.gateway.exceptions.GatewayErrorMapperImpl.ERROR_MAPPER;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.exceptions.ErrorService;
import io.scalecube.services.gateway.exceptions.ErrorServiceImpl;
import io.scalecube.services.gateway.exceptions.SomeException;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

class WebsocketLocalGatewayErrorMapperTest extends BaseTest {

  @RegisterExtension
  static WebsocketLocalGatewayExtension extension =
      new WebsocketLocalGatewayExtension(
          ServiceInfo.fromServiceInstance(new ErrorServiceImpl()).errorMapper(ERROR_MAPPER).build(),
          opts ->
              new WebsocketGateway(
                  builder ->
                      builder
                          .options(opts.call(opts.call().errorMapper(ERROR_MAPPER)))
                          .errorMapper(ERROR_MAPPER)));

  private ErrorService service;

  @BeforeEach
  void initService() {
    service = extension.client().errorMapper(ERROR_MAPPER).api(ErrorService.class);
  }

  @Test
  void shouldReturnSomeExceptionOnFlux() {
    StepVerifier.create(service.manyError()).expectError(SomeException.class).verify(TIMEOUT);
  }

  @Test
  void shouldReturnSomeExceptionOnMono() {
    StepVerifier.create(service.oneError()).expectError(SomeException.class).verify(TIMEOUT);
  }
}
