package io.scalecube.services.codec;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.BaseTest;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.jackson.JacksonCodec;
import io.scalecube.services.codec.protostuff.ProtostuffCodec;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class ServiceMessageCodecTest extends BaseTest {

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(MessageCodecProvider.class)
  void encodeAndDecode(String contentType, ServiceMessageCodec codec) {
    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(qualifier())
            .dataFormat(contentType)
            .data(data())
            .headers(headers())
            .build();

    Payload payload = codec.encodeAndTransform(message, ByteBufPayload::create);

    LOGGER.info(
        "contentType={}, headers={}, data={}",
        contentType,
        payload.getMetadataUtf8(),
        payload.getDataUtf8());

    ServiceMessage actual =
        ServiceMessageCodec.decodeData(
            codec.decode(payload.sliceData(), payload.sliceMetadata()), PlaceOrderRequest.class);

    assertAll(
        () -> assertEquals(message.qualifier(), actual.qualifier()),
        () -> assertEquals(message.headers(), actual.headers()),
        () -> assertEquals(message.dataFormat(), actual.dataFormat()),
        () -> assertEquals(message.data(), actual.data()));
  }

  private String qualifier() {
    return "io.scalecube.services.tests/SomeService/test";
  }

  private Map<String, String> headers() {
    Map<String, String> headers = new HashMap<>();
    headers.put("sid", String.valueOf(Integer.MAX_VALUE));
    headers.put("sig", String.valueOf(9));
    headers.put("inactivity", String.valueOf(Integer.MAX_VALUE));
    return headers;
  }

  private PlaceOrderRequest data() {
    PlaceOrderRequest result = new PlaceOrderRequest();
    result.orderType = "Sell";
    result.side = "Sell";
    result.instanceId = UUID.randomUUID().toString();
    result.quantity = BigDecimal.valueOf(Long.MAX_VALUE);
    result.price = BigDecimal.valueOf(Long.MAX_VALUE);
    result.isClosePositionOrder = false;
    result.requestTimestamp = LocalDateTime.now();
    result.sourceIpAddress = "255.255.255.255";
    result.token =
        "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJUZW5hbnQxIiwic3ViIjoiMSIsIm5hbWUiOiJ0cmFkZXIxIn0."
            + "j9dCs63J4xtWfhctrXb5popLAl8ohSlMTJU3_vCrQHk";
    return result;
  }

  private static class MessageCodecProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(JacksonCodec.CONTENT_TYPE, ProtostuffCodec.CONTENT_TYPE)
          .map(
              contentType ->
                  Arguments.of(
                      contentType, new ServiceMessageCodec(HeadersCodec.getInstance(contentType))));
    }
  }

  public static class PlaceOrderRequest {
    private String orderType;
    private String side;
    private String instanceId;
    private BigDecimal quantity;
    private BigDecimal price;
    private boolean isClosePositionOrder;
    private LocalDateTime requestTimestamp;
    private String token;
    private String sourceIpAddress;

    PlaceOrderRequest() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PlaceOrderRequest that = (PlaceOrderRequest) o;
      return isClosePositionOrder == that.isClosePositionOrder
          && Objects.equals(orderType, that.orderType)
          && Objects.equals(side, that.side)
          && Objects.equals(instanceId, that.instanceId)
          && Objects.equals(quantity, that.quantity)
          && Objects.equals(price, that.price)
          && Objects.equals(requestTimestamp, that.requestTimestamp)
          && Objects.equals(token, that.token)
          && Objects.equals(sourceIpAddress, that.sourceIpAddress);
    }

    @Override
    public int hashCode() {

      return Objects.hash(
          orderType,
          side,
          instanceId,
          quantity,
          price,
          isClosePositionOrder,
          requestTimestamp,
          token,
          sourceIpAddress);
    }

    @Override
    public String toString() {
      return "PlaceOrderRequest{"
          + "token='"
          + token
          + '\''
          + ", sourceIpAddress='"
          + sourceIpAddress
          + '\''
          + ", orderType='"
          + orderType
          + '\''
          + ", side='"
          + side
          + '\''
          + ", side='"
          + side
          + '\''
          + ", instanceId='"
          + instanceId
          + '\''
          + ", quantity="
          + quantity
          + ", price="
          + price
          + ", isClosePositionOrder="
          + isClosePositionOrder
          + ", requestTimestamp="
          + requestTimestamp
          + '}';
    }
  }
}
