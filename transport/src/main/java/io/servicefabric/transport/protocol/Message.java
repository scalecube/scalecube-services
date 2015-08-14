package io.servicefabric.transport.protocol;

import javax.annotation.concurrent.Immutable;

/**
 * The Class Message introduce common protocol for transport to transport communication.
 */
@Immutable
public final class Message {

	private final String qualifier;

	private final String correlationId;

	private final Object data;

	/**
	 * Instantiates a new message with given qualifier. All other parameters set to null.
	 */
	public Message(String qualifier) {
		this(qualifier, null);
	}

	/**
	 * Instantiates new messages with given qualifier and data.
	 */
	public Message(String qualifier, Object data) {
		this(qualifier, data, null);
	}

	/**
	 * Instantiates new messages with given qualifier, data and correlationId.
	 */
	public Message(String qualifier, Object data, String correlationId) {
		this.qualifier = qualifier;
		this.data = data;
		this.correlationId = correlationId;
	}

	/**
	 * Returns the message qualifier. Qualifier is used to determine the message destination and payload type.
	 *
	 * @return message qualifier
	 */
	public String qualifier() {
		return qualifier;
	}

	/**
	 * Return the message payload, which can be byte array, string or any type
	 * 
	 * @return payload of the message or null if message is without any payload
	 */
	public Object data() {
		return data;
	}

	/**
	 * Returns correlation id which can be used to match request and response. The receiver is responsible to respond
	 * with the corresponding to request correlationId.
	 *
	 * @return message correlation id or null if not specified
	 */
	public String correlationId() {
		return correlationId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof Message))
			return false;

		Message message = (Message) o;

		if (correlationId != null ? !correlationId.equals(message.correlationId) : message.correlationId != null)
			return false;
		if (data != null ? !data.equals(message.data) : message.data != null)
			return false;
		if (qualifier != null ? !qualifier.equals(message.qualifier) : message.qualifier != null)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = qualifier != null ? qualifier.hashCode() : 0;
		result = 31 * result + (correlationId != null ? correlationId.hashCode() : 0);
		result = 31 * result + (data != null ? data.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Message [qualifier=" + qualifier + ", correlationId=" + correlationId + "]";
	}

}
