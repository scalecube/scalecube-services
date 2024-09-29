package io.scalecube.services.gateway.http;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;
import static io.scalecube.services.gateway.TestUtils.TIMEOUT;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.ErrorService;
import io.scalecube.services.gateway.ErrorServiceImpl;
import io.scalecube.services.gateway.SomeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

@Disabled("Cannot deserialize instance of `java.lang.String` out of START_OBJECT token")
class HttpClientErrorMapperTest extends BaseTest {

  @RegisterExtension
  static HttpGatewayExtension extension =
      new HttpGatewayExtension(
          ServiceInfo.fromServiceInstance(new ErrorServiceImpl())
              .errorMapper(ERROR_MAPPER)
              .build());

  private ErrorService service;

  @BeforeEach
  void initService() {
    service = extension.client().errorMapper(ERROR_MAPPER).api(ErrorService.class);
  }

  @Test
  void shouldReturnSomeExceptionOnMono() {
    StepVerifier.create(service.oneError()).expectError(SomeException.class).verify(TIMEOUT);
  }
}
