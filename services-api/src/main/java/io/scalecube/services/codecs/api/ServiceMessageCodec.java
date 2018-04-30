package io.scalecube.services.codecs.api;

import io.scalecube.services.api.ServiceMessage;

import io.netty.buffer.ByteBuf;


public interface ServiceMessageCodec {

  ByteBuf[] encodeMessage(ServiceMessage message);

  ServiceMessage decodeMessage(ByteBuf dataBuf, ByteBuf headersBuf);

  ServiceMessage encodeData(ServiceMessage message);

  ServiceMessage decodeData(ServiceMessage message, Class<?> requestType);

  String contentType();

}
