package io.scalecube.services.benchmarks.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.codec.jackson.JacksonCodec;
import io.scalecube.services.codec.protostuff.ProtostuffCodec;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class SmCodecBenchmarkState extends BenchmarkState<SmCodecBenchmarkState> {

  private ServiceMessageCodec serviceMessageCodec;
  private HeadersCodec headersCodec;
  private DataCodec dataCodec;
  private ServiceMessage serviceMessage;
  private Payload payloadMessage;

  /**
   * State for benchmarking {@link ServiceMessageCodec}.
   *
   * @param settings - setting of this benchmark test.
   * @param dataCodec - data codec under test.
   * @param headersCodec - headers codec under test.
   */
  public SmCodecBenchmarkState(
      BenchmarkSettings settings, DataCodec dataCodec, HeadersCodec headersCodec) {
    super(settings);
    this.dataCodec = dataCodec;
    this.headersCodec = headersCodec;
  }

  @Override
  protected void beforeAll() {
    this.serviceMessageCodec = new ServiceMessageCodec(headersCodec);
    this.serviceMessage = generateServiceMessage(generateData());
    this.payloadMessage = generatePayload(serviceMessage);
  }

  public ServiceMessageCodec messageCodec() {
    return serviceMessageCodec;
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
    return ServiceMessage.from(serviceMessage).data(payloadMessage.sliceData()).build();
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
        "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJUZW5hbnQxIiwic3ViIjoiMSIsIm5hbWUiOiJ0cmFkZXIxIn0."
            + "j9dCs63J4xtWfhctrXb5popLAl8ohSlMTJU3_vCrQHk";
    return result;
  }

  private ServiceMessage generateServiceMessage(Object data) {
    return ServiceMessage.builder()
        .dataFormat(dataCodec.contentType())
        .qualifier("io.scalecube.services.benchmarks/SomeBenchmarkService/benchmark")
        .header("sid", String.valueOf(Integer.MAX_VALUE))
        .header("sig", String.valueOf(9))
        .header("inactivity", String.valueOf(Integer.MAX_VALUE))
        .data(data)
        .build();
  }

  private Payload generatePayload(ServiceMessage msg) {
    try {
      ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
      dataCodec.encode(dataStream, msg.data());
      System.out.println("generated dataBuffer: " + dataStream.toString());
      ByteBuf dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      dataBuffer.writeBytes(dataStream.toByteArray());

      ByteArrayOutputStream headersStream = new ByteArrayOutputStream();
      headersCodec.encode(headersStream, msg.headers());
      System.out.println("generated headersBuffer: " + headersStream.toString());
      ByteBuf headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      headersBuffer.writeBytes(headersStream.toByteArray());

      return ByteBufPayload.create(dataBuffer, headersBuffer);
    } catch (Throwable t) {
      throw new RuntimeException(t.getMessage(), t);
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

  public static class Jackson extends SmCodecBenchmarkState {

    private static final JacksonCodec CODEC = new JacksonCodec();

    public Jackson(BenchmarkSettings settings) {
      super(settings, CODEC, CODEC);
    }
  }

  public static class Protostuff extends SmCodecBenchmarkState {

    private static final ProtostuffCodec CODEC = new ProtostuffCodec();

    public Protostuff(BenchmarkSettings settings) {
      super(settings, CODEC, CODEC);
    }
  }
}
