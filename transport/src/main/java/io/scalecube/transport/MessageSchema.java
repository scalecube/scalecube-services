package io.scalecube.transport;

import static io.protostuff.LinkedBuffer.MIN_BUFFER_SIZE;
import static io.scalecube.transport.RecyclableLinkedBuffer.DEFAULT_MAX_CAPACITY;

import com.google.common.collect.ImmutableMap;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protostuff schema for {@link Message}.
 * 
 * @author Anton Kharenko
 */
final class MessageSchema implements Schema<Message> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageSchema.class);

  private static final int HEADER_KEYS_FIELD_NUMBER = 1;
  private static final int HEADER_VALUES_FIELD_NUMBER = 2;
  private static final int DATA_FIELD_NUMBER = 3;
  private static final int SENDER_HOST_FIELD_NUMBER = 4;
  private static final int SENDER_PORT_FIELD_NUMBER = 5;

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer =
      new RecyclableLinkedBuffer(MIN_BUFFER_SIZE, DEFAULT_MAX_CAPACITY);

  private static final Map<String, Integer> fieldMap = ImmutableMap.<String, Integer>builder()
      .put("headerKeys", HEADER_KEYS_FIELD_NUMBER)
      .put("headerValues", HEADER_VALUES_FIELD_NUMBER)
      .put("data", DATA_FIELD_NUMBER)
      .put("senderHost", SENDER_HOST_FIELD_NUMBER)
      .put("senderPort", SENDER_PORT_FIELD_NUMBER)
      .build();

  private final Map<String, Optional<Class>> classCache = new ConcurrentHashMap<>();

  @Override
  public String getFieldName(int number) {
    switch (number) {
      case HEADER_KEYS_FIELD_NUMBER:
        return "headerKeys";
      case HEADER_VALUES_FIELD_NUMBER:
        return "headerValues";
      case DATA_FIELD_NUMBER:
        return "data";
      case SENDER_HOST_FIELD_NUMBER:
        return "senderHost";
      case SENDER_PORT_FIELD_NUMBER:
        return "senderPort";
      default:
        return null;
    }
  }

  @Override
  public int getFieldNumber(String name) {
    return fieldMap.get(name);
  }

  @Override
  public boolean isInitialized(Message message) {
    return message != null;
  }

  @Override
  public Message newMessage() {
    return new Message();
  }

  @Override
  public String messageName() {
    return Message.class.getSimpleName();
  }

  @Override
  public String messageFullName() {
    return Message.class.getName();
  }

  @Override
  public Class<? super Message> typeClass() {
    return Message.class;
  }

  @Override
  public void mergeFrom(Input input, Message message) throws IOException {
    // Read input data
    boolean iterate = true;
    List<String> headerKeys = new ArrayList<>();
    List<String> headerValues = new ArrayList<>();
    String senderHost = null;
    int senderPort = 0;
    byte[] dataBytes = null;
    while (iterate) {
      int number = input.readFieldNumber(this);
      switch (number) {
        case 0:
          iterate = false;
          break;
        case HEADER_KEYS_FIELD_NUMBER:
          headerKeys.add(input.readString());
          break;
        case HEADER_VALUES_FIELD_NUMBER:
          headerValues.add(input.readString());
          break;
        case DATA_FIELD_NUMBER:
          dataBytes = input.readByteArray();
          break;
        case SENDER_HOST_FIELD_NUMBER:
          senderHost = input.readString();
          break;
        case SENDER_PORT_FIELD_NUMBER:
          senderPort = input.readInt32();
          break;
        default:
          input.handleUnknownField(number, this);
          break;
      }
    }

    // Deserialize headers
    Map<String, String> headers = new HashMap<>(headerKeys.size());
    if (!headerKeys.isEmpty()) {
      ListIterator<String> headerValuesIterator = headerValues.listIterator();
      for (String key : headerKeys) {
        String value = headerValuesIterator.next();
        headers.put(key, value);
      }
    }

    // Deserialize data
    Object data = null;
    if (dataBytes != null) {
      String dataType = headers.get(Message.HEADER_DATA_TYPE);
      if (dataType == null) {
        data = dataBytes;
      } else {
        Optional<Class> optionalDataClass = classCache.computeIfAbsent(dataType, this::classForName);
        if (optionalDataClass.isPresent()) {
          headers.remove(Message.HEADER_DATA_TYPE);
          Class<?> dataClass = optionalDataClass.get();
          Schema dataSchema = RuntimeSchema.getSchema(dataClass);
          data = dataSchema.newMessage();
          try {
            ProtostuffIOUtil.mergeFrom(dataBytes, data, dataSchema);
          } catch (Throwable e) {
            LOGGER.error("Failed to deserialize : {}", message);
            throw e;
          }
        } else {
          data = dataBytes;
        }
      }
    }

    // Deserialize sender
    Address sender = senderHost != null ? Address.create(senderHost, senderPort) : null;

    // Set message
    message.setHeaders(headers);
    message.setData(data);
    message.setSender(sender);
  }

  @Override
  public void writeTo(Output output, Message message) throws IOException {
    // Write headers
    if (!message.headers().isEmpty()) {
      for (Map.Entry<String, String> headerEntry : message.headers().entrySet()) {
        if (headerEntry.getKey() != null && headerEntry.getValue() != null) {
          output.writeString(HEADER_KEYS_FIELD_NUMBER, headerEntry.getKey(), true);
          output.writeString(HEADER_VALUES_FIELD_NUMBER, headerEntry.getValue(), true);
        }
      }
    }

    // Write data
    Object originalData = message.data();
    if (originalData != null) {
      if (originalData instanceof byte[]) {
        // Write data byte array as is
        output.writeByteArray(DATA_FIELD_NUMBER, (byte[]) originalData, false);
      } else {
        // Write data class as an additional header
        Class<?> dataClass = originalData.getClass();
        output.writeString(HEADER_KEYS_FIELD_NUMBER, Message.HEADER_DATA_TYPE, true);
        output.writeString(HEADER_VALUES_FIELD_NUMBER, dataClass.getName(), true);

        // Write data as serialized byte array
        Schema dataSchema = RuntimeSchema.getSchema(dataClass);
        try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
          byte[] array = ProtostuffIOUtil.toByteArray(originalData, dataSchema, rlb.buffer());
          output.writeByteArray(DATA_FIELD_NUMBER, array, false);
        }
      }
    }

    // Write sender
    Address sender = message.sender();
    if (sender != null) {
      output.writeString(SENDER_HOST_FIELD_NUMBER, sender.host(), false);
      output.writeInt32(SENDER_PORT_FIELD_NUMBER, sender.port(), false);
    }
  }

  private Optional<Class> classForName(String className) {
    try {
      Class dataClass = Class.forName(className);
      return Optional.of(dataClass);
    } catch (ClassNotFoundException e) {
      return Optional.empty();
    }
  }
}
