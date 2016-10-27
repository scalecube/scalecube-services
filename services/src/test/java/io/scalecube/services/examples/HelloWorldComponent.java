package io.scalecube.services.examples;

import java.util.concurrent.CompletableFuture;

public class HelloWorldComponent implements GreetingService {
	@Override
	public String greeting(String name) {
		return " hello to: " + name;
	}

	@Override
	public CompletableFuture<String> asyncGreeting(String name) {
		return CompletableFuture.completedFuture(" hello to: " + name);
	}

}
