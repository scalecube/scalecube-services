package io.scalecube.gateway.http;

import static io.scalecube.ipc.ErrorData.ERROR_CODE_NAME;
import static io.scalecube.ipc.ErrorData.MESSAGE_NAME;
import static io.scalecube.ipc.Qualifier.ERROR_NAMESPACE;

import io.scalecube.ipc.ErrorData;
import io.scalecube.ipc.Qualifier;
import io.scalecube.ipc.codec.JsonCodec;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

import java.util.Collections;
import java.util.List;

public final class HttpCodecUtil {

  private static final List<String> ERROR_DATA_FIELDS = ImmutableList.of(ERROR_CODE_NAME, MESSAGE_NAME);

  private HttpCodecUtil() {
    // Do not instantiate
  }

  /**
   * Creates empty response.
   */
  public static FullHttpResponse emptyResponse() {
    return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT, Unpooled.EMPTY_BUFFER);
  }

  /**
   * Creates ok http response with content.
   */
  public static FullHttpResponse okResponse(ByteBuf body) {
    return toHttpResponse(HttpResponseStatus.OK, body);
  }

  /**
   * Constructs http response out of status and error data.
   */
  public static FullHttpResponse errorResponse(Qualifier qualifier, ErrorData errordata) {
    return errorResponse(qualifier, encodeErrorData(errordata));
  }

  /**
   * Constructs http response out of status and content.
   */
  public static FullHttpResponse errorResponse(Qualifier qualifier, ByteBuf buf) {
    return toHttpResponse(toErrorStatus(qualifier), buf);
  }

  /**
   * Constructs http error response with empty buffer.
   */
  public static FullHttpResponse emptyErrorResponse(Qualifier qualifier) {
    return toHttpResponse(toErrorStatus(qualifier), Unpooled.EMPTY_BUFFER);
  }

  /**
   * Constructs http response out of status and content.
   */
  public static FullHttpResponse toHttpResponse(HttpResponseStatus status, ByteBuf content) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    // in case of adding not simple headers in response they need to be specified at Access-Control-Expose-Headers
    // header
    HttpUtil.setKeepAlive(response, true); // HTTP1/1 only
    HttpUtil.setContentLength(response, content.readableBytes());
    return response;
  }

  /**
   * Constructs http error response out of message qualifier.
   */
  public static HttpResponseStatus toErrorStatus(Qualifier qualifier) {
    if (!ERROR_NAMESPACE.equalsIgnoreCase(qualifier.getNamespace())) {
      throw new IllegalArgumentException("Not an error qualifier: " + qualifier);
    }
    int code;
    try {
      code = Integer.valueOf(qualifier.getAction());
    } catch (Exception throwable) {
      throw new IllegalArgumentException("Not an error qualifier: " + qualifier);
    }
    return HttpResponseStatus.valueOf(code);
  }

  private static ByteBuf encodeErrorData(ErrorData errorData) {
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
    try {
      JsonCodec.encode(buf, ERROR_DATA_FIELDS, Collections.emptyList(), fieldName -> {
        switch (fieldName) {
          case ERROR_CODE_NAME:
            return errorData.getErrorCode();
          case MESSAGE_NAME:
            return errorData.getMessage();
          default:
            return null;
        }
      });
      return buf;
    } catch (Exception e) {
      buf.release();
      return Unpooled.EMPTY_BUFFER; // dont rethrow
    }
  }
}
