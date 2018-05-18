package io.scalecube.gateway.websocket;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Iterator;

public class JsonCodec {
  private static final String DATA = "d";
  private static final String HEADERS = "h";
  private static final String QUALIFIER = "q";
  private static final String APPLICATION_JSON = "application/json";
  private static ObjectMapper mapper = new ObjectMapper();
  {
    mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(Visibility.ANY)
        .withGetterVisibility(Visibility.NONE)
        .withSetterVisibility(Visibility.NONE)
        .withCreatorVisibility(Visibility.NONE));
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  }

  public ServiceMessage decodeServiceMessage(ByteBuf content) throws Exception {
    byte[] bytes = new byte[content.readableBytes()];
    int readerIndex = content.readerIndex();
    content.getBytes(readerIndex, bytes);
    JsonNode node = mapper.readValue(bytes, JsonNode.class);

    Builder builder = ServiceMessage.builder().dataFormat(APPLICATION_JSON);
    
    if (node.has(QUALIFIER)) {
      builder.qualifier(node.get(QUALIFIER).asText());
    }

    if (node.has(HEADERS)) {
      Iterator<String> names = node.get(HEADERS).fieldNames();
      while (names.hasNext()) {
        String key = names.next();
        builder.header(key, node.get(HEADERS).get(key).asText());
      }
    }

    if (node.has(DATA)) {
      builder.data(Unpooled.copiedBuffer(this.encode(node.get(DATA))));
    }

    return builder.build();
  }

  public byte[] encode(JsonNode jsonNode) throws JsonProcessingException {
    return mapper.writeValueAsBytes(jsonNode);
  }

}
