package io.servicefabric.transport.protocol;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The Class Message introduces generic protocol used for point to point communication by transport.
 */
public final class Message {

	private Map<String, String> headers = Collections.emptyMap();

	private Object data;

	/**
	 * Instantiates empty message for deserialization purpose.
	 */
	Message() {
	}

	/**
	 * Instantiates a new message with the given data and without headers.
	 */
	public Message(Object data) {
		setData(data);
	}

	/**
	 * Instantiates a new message with the given headers and with empty data.
	 */
	public Message(Map<String, String> headers) {
		setHeaders(headers);
	}

	/**
	 * Instantiates new messages with given qualifier, data and correlationId.
	 */
	public Message(Object data, Map<String, String> headers) {
		setData(data);
		setHeaders(headers);
	}

	/**
	 * Instantiates new message with given qualifier and headers. Headers passed a sequence of key-value pairs.
	 */
	public Message(Object data, String... headers) {
		Preconditions.checkArgument(headers.length % 2 == 0, "");
		Map<String, String> headersMap = new HashMap<>(headers.length / 2);
		for (int i = 0; i < headers.length; i += 2) {
			headersMap.put(headers[i], headers[i + 1]);
		}
		this.headers = Collections.unmodifiableMap(headersMap);
		setData(data);
	}

	void setData(Object data) {
		this.data = data;
	}

	void setHeaders(Map<String, String> headers) {
		if (headers != null) {
			this.headers = Collections.unmodifiableMap(headers);
		}
	}

	/**
	 * Returns the message headers.
	 *
	 * @return message headers
	 */
	public Map<String, String> headers() {
		return headers;
	}

	/**
	 * Returns the message header by given header name.
	 */
	public String header(String headerName) {
		return headers.get(headerName);
	}

	/**
	 * Return the message data, which can be byte array, string or any type
	 * 
	 * @return payload of the message or null if message is without any payload
	 */
	public Object data() {
		return data;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Message that = (Message) o;
		return Objects.equals(headers, that.headers) &&
				Objects.equals(data, that.data);
	}

	@Override
	public int hashCode() {
		return Objects.hash(headers, data);
	}

	@Override
	public String toString() {
		return "Message{" +
				"headers=" + headers +
				", data=" + (data == null ? "null" : data.getClass().getSimpleName()) +
				'}';
	}

}
