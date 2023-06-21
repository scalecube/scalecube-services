package io.scalecube.services.gateway.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.transport.api.DataCodec;
import java.lang.reflect.Type;

public final class ServiceMessageCodec {

  private ServiceMessageCodec() {}

  /**
   * Decode message.
   *
   * @param message the original message (with {@link ByteBuf} data)
   * @param dataType the type of the data.
   * @return a new Service message that upon {@link ServiceMessage#data()} returns the actual data
   *     (of type data type)
   * @throws MessageCodecException when decode fails
   */
  public static ServiceMessage decodeData(ServiceMessage message, Type dataType)
      throws MessageCodecException {
    if (dataType == null
        || !message.hasData(ByteBuf.class)
        || ((ByteBuf) message.data()).readableBytes() == 0
        || ByteBuf.class == dataType) {
      return message;
    }

    Object data;
    Type targetType = message.isError() ? ErrorData.class : dataType;

    ByteBuf dataBuffer = message.data();
    try (ByteBufInputStream inputStream = new ByteBufInputStream(dataBuffer, true)) {
      DataCodec dataCodec = DataCodec.getInstance(message.dataFormatOrDefault());
      data = dataCodec.decode(inputStream, targetType);
    } catch (Throwable ex) {
      throw new MessageCodecException("Failed to decode service message data", ex);
    }

    return ServiceMessage.from(message).data(data).build();
  }
}
