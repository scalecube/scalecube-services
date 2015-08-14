package io.servicefabric.transport;

import io.netty.channel.Channel;

public interface PipelineFactory {

	void setAcceptorPipeline(Channel channel, ITransportSpi transportSpi);

	void setConnectorPipeline(Channel channel, ITransportSpi transportSpi);

	void resetDueHandshake(Channel channel, ITransportSpi transportSpi);
}
