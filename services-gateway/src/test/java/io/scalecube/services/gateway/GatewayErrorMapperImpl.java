package io.scalecube.services.gateway;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;

public class GatewayErrorMapperImpl
    implements ServiceProviderErrorMapper, ServiceClientErrorMapper {

  public static final GatewayErrorMapperImpl ERROR_MAPPER = new GatewayErrorMapperImpl();

  @Override
  public Throwable toError(ServiceMessage message) {
    if (SomeException.ERROR_TYPE == message.errorType()) {
      final ErrorData data = message.data();
      if (SomeException.ERROR_CODE == data.getErrorCode()) {
        return new SomeException();
      }
    }
    return DefaultErrorMapper.INSTANCE.toError(message);
  }

  @Override
  public ServiceMessage toMessage(String qualifier, Throwable throwable) {
    if (throwable instanceof SomeException) {
      final int errorCode = ((SomeException) throwable).errorCode();
      final int errorType = SomeException.ERROR_TYPE;
      final String errorMessage = throwable.getMessage();
      return ServiceMessage.error(qualifier, errorType, errorCode, errorMessage);
    }
    return DefaultErrorMapper.INSTANCE.toMessage(qualifier, throwable);
  }
}
