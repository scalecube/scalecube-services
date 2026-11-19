package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.api.ServiceMessage.HEADER_ERROR_TYPE;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;

public final class GatewayMessages {

  static final String QUALIFIER_FIELD = "q";
  static final String STREAM_ID_FIELD = "sid";
  static final String DATA_FIELD = "d";
  static final String SIGNAL_FIELD = "sig";
  static final String INACTIVITY_FIELD = "i";
  static final String RATE_LIMIT_FIELD = "rlimit";

  private GatewayMessages() {
    // Do not instantiate
  }

  /**
   * Returns cancel message by given arguments.
   *
   * @param sid sid
   * @param qualifier qualifier
   * @return {@link ServiceMessage} instance as the cancel signal
   */
  public static ServiceMessage newCancelMessage(long sid, String qualifier) {
    return ServiceMessage.builder()
        .qualifier(qualifier)
        .header(STREAM_ID_FIELD, sid)
        .header(SIGNAL_FIELD, Signal.CANCEL.code())
        .build();
  }

  /**
   * Returns error message by given arguments.
   *
   * @param errorMapper error mapper
   * @param request request
   * @param th cause
   * @return {@link ServiceMessage} instance as the error signal
   */
  public static ServiceMessage toErrorResponse(
      ServiceProviderErrorMapper errorMapper, ServiceMessage request, Throwable th) {

    final String qualifier = request.qualifier() != null ? request.qualifier() : "scalecube/error";
    final String sid = request.header(STREAM_ID_FIELD);
    final ServiceMessage errorMessage = errorMapper.toMessage(qualifier, th);

    // sid is encoded as a number, so an invalid one can't be echoed back
    if (sid == null || !isNumber(sid)) {
      return ServiceMessage.from(errorMessage).header(SIGNAL_FIELD, Signal.ERROR.code()).build();
    }

    return ServiceMessage.from(errorMessage)
        .header(SIGNAL_FIELD, Signal.ERROR.code())
        .header(STREAM_ID_FIELD, sid)
        .build();
  }

  /**
   * Returns complete message by given arguments.
   *
   * @param sid sid
   * @param qualifier qualifier
   * @return {@link ServiceMessage} instance as the complete signal
   */
  public static ServiceMessage newCompleteMessage(long sid, String qualifier) {
    return ServiceMessage.builder()
        .qualifier(qualifier)
        .header(STREAM_ID_FIELD, sid)
        .header(SIGNAL_FIELD, Signal.COMPLETE.code())
        .build();
  }

  /**
   * Returns response message by given arguments.
   *
   * @param sid sid
   * @param message request
   * @param isErrorResponse should the message be marked as an error?
   * @return {@link ServiceMessage} instance as the response
   */
  public static ServiceMessage newResponseMessage(
      long sid, ServiceMessage message, boolean isErrorResponse) {
    final var builder =
        ServiceMessage.builder()
            .qualifier(message.qualifier())
            .data(message.data())
            .header(STREAM_ID_FIELD, sid);

    if (message.propagateDataType()) {
      builder.dataType(message.dataType());
    }

    if (isErrorResponse) {
      return builder
          .header(HEADER_ERROR_TYPE, message.errorType())
          .header(SIGNAL_FIELD, Signal.ERROR.code())
          .build();
    }

    return builder.build();
  }

  /**
   * Verifies the sid existence in a given message.
   *
   * @param message message
   * @return incoming message
   */
  public static ServiceMessage validateSid(ServiceMessage message) {
    final String sid = message.header(STREAM_ID_FIELD);
    if (sid == null) {
      throw WebsocketContextException.badRequest("sid is missing", message);
    }
    if (!isNumber(sid)) {
      throw WebsocketContextException.badRequest("sid is invalid: " + sid, message);
    }
    return message;
  }

  /**
   * Verifies the signal, if present, is a known signal code.
   *
   * @param message message
   * @return incoming message
   */
  public static ServiceMessage validateSignal(ServiceMessage message) {
    final String sig = message.header(SIGNAL_FIELD);
    if (sig == null) {
      return message;
    }
    try {
      Signal.from(Integer.parseInt(sig));
    } catch (IllegalArgumentException ex) {
      throw WebsocketContextException.badRequest("sig is invalid: " + sig, message);
    }
    return message;
  }

  /**
   * Verifies the rate limit, if present, is a positive integer.
   *
   * @param message message
   * @return incoming message
   */
  public static ServiceMessage validateRateLimit(ServiceMessage message) {
    final String rlimit = message.header(RATE_LIMIT_FIELD);
    if (rlimit == null) {
      return message;
    }
    int value;
    try {
      value = Integer.parseInt(rlimit);
    } catch (NumberFormatException ex) {
      value = 0;
    }
    if (value <= 0) {
      throw WebsocketContextException.badRequest("rlimit is invalid: " + rlimit, message);
    }
    return message;
  }

  /**
   * Verifies the sid is not used in a given session.
   *
   * @param session session
   * @param message message
   * @return incoming message
   */
  public static ServiceMessage validateSidOnSession(
      WebsocketGatewaySession session, ServiceMessage message) {
    long sid = getSid(message);
    if (session.containsSid(sid)) {
      throw WebsocketContextException.badRequest("sid=" + sid + " is already registered", message);
    } else {
      return message;
    }
  }

  /**
   * Verifies the qualifier existence in a given message.
   *
   * @param message message
   * @return incoming message
   */
  public static ServiceMessage validateQualifier(ServiceMessage message) {
    if (message.qualifier() == null) {
      throw WebsocketContextException.badRequest("qualifier is missing", message);
    }
    return message;
  }

  /**
   * Returns sid from a given message.
   *
   * @param message message
   * @return sid
   */
  public static long getSid(ServiceMessage message) {
    return Long.parseLong(message.header(STREAM_ID_FIELD));
  }

  /**
   * Returns signal from a given message.
   *
   * @param message message
   * @return signal
   */
  public static Signal getSignal(ServiceMessage message) {
    String header = message.header(SIGNAL_FIELD);
    return header != null ? Signal.from(Integer.parseInt(header)) : null;
  }

  private static boolean isNumber(String value) {
    try {
      Long.parseLong(value);
      return true;
    } catch (NumberFormatException ex) {
      return false;
    }
  }
}
