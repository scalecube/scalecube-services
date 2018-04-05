package io.scalecube.gateway.http;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_MAX_AGE;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.streams.ErrorData;
import io.scalecube.streams.Event;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.ServerStream;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.netty.ChannelSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import rx.Subscription;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class GatewayHttpServerTest {

  private static final int HTTP_SERVER_PORT = 8080;

  private static final String EXPECTED_ACCESS_CONTROL_ALLOW_ORIGIN = "*";
  private static final String EXPECTED_ACCESS_CONTROL_ALLOW_METHODS = "GET, POST, OPTIONS";
  private static final int EXPECTED_ACCESS_CONTROL_MAX_AGE = 86400;
  private static final String EXPECTED_ACCESS_CONTROL_ALLOW_HEADERS = "X-PINGOTHER, Content-Type";

  private static GatewayHttpServer gatewayHttpServer;
  private static ServerStream serverStream;
  private static AsyncHttpClient httpClient;
  private static ObjectMapper mapper = new ObjectMapper();

  private Subscription subscription;

  @BeforeClass
  public static void init() {
    serverStream = ServerStream.newServerStream();
    gatewayHttpServer = GatewayHttpServer.onPort(HTTP_SERVER_PORT, serverStream)
        .withAccessControlAllowOrigin(EXPECTED_ACCESS_CONTROL_ALLOW_ORIGIN)
        .withAccessControlAllowMethods(EXPECTED_ACCESS_CONTROL_ALLOW_METHODS)
        .withAccessControlMaxAge(EXPECTED_ACCESS_CONTROL_MAX_AGE)
        .withCorsEnabled(true);
    gatewayHttpServer.start();

    httpClient = Dsl.asyncHttpClient();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    gatewayHttpServer.stop();
    serverStream.close();

    httpClient.close();
  }

  @After
  public void tearDown() {
    if (subscription != null) {
      subscription.unsubscribe();
    }
  }

  @Test
  public void simpleServerResponse() throws Exception {
    String expectedBody = "serverResponse";
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(response(msg, expectedBody)));

    Response response = httpClient.preparePost(url()).setBody("clientRequestBody").execute().get();

    assertEquals(OK.code(), response.getStatusCode());
    assertEquals(expectedBody, response.getResponseBody());
  }


  @Test
  public void emptyResponse() throws Exception {
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(emptyResponse(msg)));

    Response response = httpClient.preparePost(url()).setBody("clientRequestBody").execute().get();

    assertEquals(NO_CONTENT.code(), response.getStatusCode());
    assertFalse(response.hasResponseBody());
  }

  @Test
  public void echoResponse() throws Exception {
    String expectedBody = "clientRequestBody";
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(msg));

    Response response = httpClient.preparePost(url()).setBody(expectedBody).execute().get();

    assertEquals(OK.code(), response.getStatusCode());
    assertEquals(expectedBody, response.getResponseBody());
  }

  @Test
  public void requestWithQualifier() throws Exception {
    String expectedQuilifier = "qualifier/test";
    CompletableFuture<StreamMessage> promise = new CompletableFuture<>();
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> {
          promise.complete(msg);
          serverStream.send(emptyResponse(msg));
        });

    Response response = httpClient.preparePost(url(expectedQuilifier)).setBody("clientRequestBody").execute().get();

    assertEquals(NO_CONTENT.code(), response.getStatusCode());
    assertFalse(response.hasResponseBody());
    StreamMessage message = promise.get();
    assertEquals(expectedQuilifier, message.qualifier());
  }

  @Test
  public void requestWithoutQualifier() throws Exception {
    CompletableFuture<StreamMessage> promise = new CompletableFuture<>();
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> {
          promise.complete(msg);
          serverStream.send(emptyResponse(msg));
        });

    Response response = httpClient.preparePost(url()).setBody("clientRequestBody").execute().get();

    assertEquals(NO_CONTENT.code(), response.getStatusCode());
    assertFalse(response.hasResponseBody());
    StreamMessage message = promise.get();
    assertEquals("", message.qualifier());
  }

  @Test
  public void emptyErrorResponse() throws Exception {
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(emptyErrorResponse(msg)));

    Response response = httpClient.preparePost(url()).setBody("clientRequestBody").execute().get();

    assertEquals(INTERNAL_SERVER_ERROR.code(), response.getStatusCode());
    assertFalse(response.hasResponseBody());
  }

  @Test
  public void errorResponseWithByteBuf() throws Exception {
    String expectedBody = "serverResponse";
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(errorResponse(msg, expectedBody)));

    Response response = httpClient.preparePost(url()).setBody("clientRequestBody").execute().get();

    assertEquals(INTERNAL_SERVER_ERROR.code(), response.getStatusCode());
    assertEquals(expectedBody, response.getResponseBody());
  }

  @Test
  public void errorResponseWithErrorData() throws Exception {
    ErrorData expectedData = new ErrorData(101, "alert!");
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(errorResponse(msg, expectedData)));

    Response response = httpClient.preparePost(url()).setBody("clientRequestBody").execute().get();

    assertEquals(INTERNAL_SERVER_ERROR.code(), response.getStatusCode());
    assertEquals(expectedData, toErrorData(response.getResponseBody()));
  }

  @Test
  public void optionsRequest() throws Exception {
    subscription = serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(msg -> serverStream.send(emptyResponse(msg)));

    Response response = httpClient.prepareOptions(url())
        .setHeader(ACCESS_CONTROL_REQUEST_HEADERS, EXPECTED_ACCESS_CONTROL_ALLOW_HEADERS)
        .execute().get();

    assertEquals(OK.code(), response.getStatusCode());
    assertFalse(response.hasResponseBody());
    assertTrue(response.hasResponseHeaders());
    assertEquals(EXPECTED_ACCESS_CONTROL_ALLOW_ORIGIN, response.getHeader(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN));
    assertEquals(EXPECTED_ACCESS_CONTROL_ALLOW_METHODS, response.getHeader(ACCESS_CONTROL_ALLOW_METHODS));
    assertEquals(String.valueOf(EXPECTED_ACCESS_CONTROL_MAX_AGE), response.getHeader(ACCESS_CONTROL_MAX_AGE));
    assertEquals(EXPECTED_ACCESS_CONTROL_ALLOW_HEADERS, response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS));
  }

  private String url() {
    return "http://0.0.0.0:" + HTTP_SERVER_PORT;
  }

  private String url(String quilifier) {
    return "http://0.0.0.0:" + HTTP_SERVER_PORT + "/" + quilifier;
  }

  private StreamMessage emptyResponse(StreamMessage message) {
    ChannelSupport.releaseRefCount(message.data());
    return StreamMessage.from(message).data(null).build();
  }

  private StreamMessage response(StreamMessage message, String body) {
    ChannelSupport.releaseRefCount(message.data());
    return StreamMessage.from(message)
        .data(Unpooled.buffer().writeBytes(body.getBytes())).build();
  }

  private StreamMessage emptyErrorResponse(StreamMessage message) {
    ChannelSupport.releaseRefCount(message.data());
    return StreamMessage.from(message)
        .qualifier(Qualifier.Q_GENERAL_FAILURE)
        .data(null).build();
  }

  private StreamMessage errorResponse(StreamMessage message, String body) {
    ChannelSupport.releaseRefCount(message.data());
    return StreamMessage.from(message)
        .qualifier(Qualifier.Q_GENERAL_FAILURE)
        .data(Unpooled.buffer().writeBytes(body.getBytes())).build();
  }

  private StreamMessage errorResponse(StreamMessage message, ErrorData data) {
    ChannelSupport.releaseRefCount(message.data());
    return StreamMessage.from(message)
        .qualifier(Qualifier.Q_GENERAL_FAILURE)
        .data(data).build();
  }

  private ErrorData toErrorData(String raw) throws IOException {
    JsonNode node = mapper.readTree(raw);
    int errorCode = node.get("errorCode").asInt();
    String message = node.get("message").asText();
    return new ErrorData(errorCode, message);
  }
}
