# New Transport API for scalecube

## Features

* Separated RSocket Transport and Tcp Netty Transport

* Easy configuration

## API Changes 

### RSocketTransport:

Creation of transport is taken out of RSocketTransport:

* ClientTransport => ClientTransportFactory

* ServerTransport => ServerTransportFactory

### Scalecube Transport:
    
ServiceTransportProvider and builders mechanism added to create and configure specific 
implementations, example `RSocketByNettyTcp`.

## API Examples

```java
ServiceTransportProvider tcpRSocketProvider = RSocketNettyTcp
        .builder()
        .customizeClient(TcpClient::noProxy)
        .customizeClient(TcpClient::secure)
        .customizeServer(tcpServer -> tcpServer.wiretap(true))
        .customizeServer(TcpServer::noSSL)
        .dataCodecs(new JacksonCodec(), new ProtostuffCodec())
        // or
        .dataCodecs(asList(new JacksonCodec(), new ProtostuffCodec()))
        .headersCodec(new JacksonCodec())
        .build();

Microservices seed =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(opt -> opt
                .transportProvider(tcpRSocketProvider)
                .host("host")
                .port(9000)
            )
            .startAwait();
```