package io.scalecube.services;

public enum CommunicationMode {
  /** Corresponds to <code>Mono&lt;Void&gt; action(REQ)</code>. */
  FIRE_AND_FORGET,
  /** Corresponds to <code>Mono&lt;RESP&gt; action(REQ)</code>. */
  REQUEST_RESPONSE,
  /** Corresponds to <code>Flux&lt;RESP&gt; action(REQ)</code>. */
  REQUEST_STREAM,
  /** Corresponds to <code>Flux&lt;RESP&gt; action(Flux&lt;REQ&gt;)</code>. */
  REQUEST_CHANNEL;
}
