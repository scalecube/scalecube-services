package io.servicefabric.transport.protocol.protostuff;

import io.protostuff.Tag;

import java.util.Arrays;

/**
 * Message wrapper class which used for serialization and deserialization of message in two steps.
 * First, for headers and second, for data. This class represents payload as a byte array.
 * 
 * @author <a href="mailto:Anton.Kharenko@playtech.com">Anton Kharenko</a>
 */
class BinaryMessage {

	@Tag(1)
	private String qualifier;

	@Tag(2)
	private String correlationId;

	@Tag(3)
	private byte[] data;

	@Tag(4)
	private String dataClass;

	/**
	 * Instantiates a new binary message wrapper.
	 */
	BinaryMessage() {
	}

	/**
	 * Gets the qualifier.
	 *
	 * @return the qualifier
	 */
	public String getQualifier() {
		return qualifier;
	}

	/**
	 * Sets the qualifier.
	 *
	 * @param qualifier the new qualifier
	 */
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	/**
	 * Gets the data.
	 *
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * Sets the data.
	 *
	 * @param data the new data
	 */
	public void setData(byte[] data) {
		this.data = data;
	}

	/**
	 * Gets the data class. It internal field used for deserialization of data.
	 *
	 * @return the data class
	 */
	public String getDataClass() {
		return dataClass;
	}

	/**
	 * Sets the data class. It is internal field used for deserialization of data.
	 *
	 * @param dataClass the data class
	 */
	public void setDataClass(String dataClass) {
		this.dataClass = dataClass;
	}

	@Override
	public String toString() {
		return "BinaryMessage{" +
				"qualifier='" + qualifier + '\'' +
				", correlationId='" + correlationId + '\'' +
				", data=***" +
				", dataClass='" + dataClass + '\'' +
				'}';
	}
}
