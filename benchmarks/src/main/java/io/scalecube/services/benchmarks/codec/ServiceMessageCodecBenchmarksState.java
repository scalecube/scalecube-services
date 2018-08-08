package io.scalecube.services.benchmarks.codec;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.BenchmarksState;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.codec.jackson.JacksonCodec;
import io.scalecube.services.codec.protostuff.ProtostuffHeadersCodec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class ServiceMessageCodecBenchmarksState extends BenchmarksState<ServiceMessageCodecBenchmarksState> {

  private ServiceMessageCodec jacksonServiceMessageCodec;

  private final ObjectMapper objectMapper = objectMapper();

  private ServiceMessage serviceMessage;
  private Payload payloadMessage;
  private HeadersCodec headersCodec;

  public ServiceMessageCodecBenchmarksState(BenchmarksSettings settings, HeadersCodec headersCodec) {
    super(settings);
    this.headersCodec = headersCodec;
  }

  @Override
  protected void beforeAll() {
    this.jacksonServiceMessageCodec = new ServiceMessageCodec(headersCodec);
    this.serviceMessage = generateServiceMessage(generateData());
    this.payloadMessage = generatePayload(serviceMessage);
  }

  public ServiceMessageCodec jacksonMessageCodec() {
    return jacksonServiceMessageCodec;
  }

  public ByteBuf dataBuffer() {
    return payloadMessage.sliceData();
  }

  public ByteBuf headersBuffer() {
    return payloadMessage.sliceMetadata();
  }

  public Class<?> dataType() {
    return PlaceOrderRequest.class;
  }

  public ServiceMessage message() {
    return serviceMessage;
  }

  public ServiceMessage messageWithByteBuf() {
    return ServiceMessage.from(serviceMessage)
        .data(payloadMessage.sliceData())
        .build();
  }

  private PlaceOrderRequest generateData() {
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
        "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJUZW5hbnQxIiwic3ViIjoiMSIsIm5hbWUiOiJ0cmFkZXIxIn0.j9dCs63J4xtWfhctrXb5popLAl8ohSlMTJU3_vCrQHk";
    return result;
  }

  private ServiceMessage generateServiceMessage(Object data) {
    return ServiceMessage.builder()
        .qualifier("io.scalecube.services.benchmarks/SomeBenchmarkService/benchmark")
        .header("sid", String.valueOf(Integer.MAX_VALUE))
        .header("sig", String.valueOf(9))
        .header("inactivity", String.valueOf(Integer.MAX_VALUE))
        .data(data)
        .build();
  }

  private Payload generatePayload(ServiceMessage msg) {
    try {
      String data = objectMapper.writeValueAsString(msg.data());
      System.out.println("generated dataBuffer: " + data);
      ByteBuf dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      dataBuffer.writeBytes(data.getBytes());


      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      headersCodec.encode(baos, msg.headers());
      System.out.println("generated headersBuffer: `" + baos.toString() + "`");
      ByteBuf headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      headersBuffer.writeBytes(baos.toByteArray());

      return ByteBufPayload.create(dataBuffer, headersBuffer);
    } catch (Throwable t) {
      throw new RuntimeException(t.getMessage(), t);
    }
  }

  private ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
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
    public String toString() {
      return "PlaceOrderRequest{" +
          "token='" + token + '\'' +
          ", sourceIpAddress='" + sourceIpAddress + '\'' +
          ", orderType='" + orderType + '\'' +
          ", side='" + side + '\'' +
          ", side='" + side + '\'' +
          ", instanceId='" + instanceId + '\'' +
          ", quantity=" + quantity +
          ", price=" + price +
          ", isClosePositionOrder=" + isClosePositionOrder +
          ", requestTimestamp=" + requestTimestamp +
          '}';
    }

  }

  public static class Jackson extends ServiceMessageCodecBenchmarksState {
    public Jackson(BenchmarksSettings settings) {
      super(settings, new JacksonCodec());
    }
  }

  public static class Protostuff extends ServiceMessageCodecBenchmarksState {
    public Protostuff(BenchmarksSettings settings) {
      super(settings, new ProtostuffHeadersCodec());
    }
  }
}
