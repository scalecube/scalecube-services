package io.scalecube.services.examples;

import org.rapidoid.io.IO;
import org.rapidoid.setup.On;

/**
 * Async echo server
 */
public class AsyncEchoServer {

	private int port = 8888;

	public AsyncEchoServer() {
	}

	public AsyncEchoServer(int port) {
		this.port = port;
	}

	public AsyncEchoServer start() {

		On.req(req -> {
			req.async();
			IO.write(req.response().out(), req.uri().substring(1));
			req.done();
			return req;
		});
		return this;
	}

	public AsyncEchoServer stop() {
		On.setup().shutdown();
		return this;
	}
}