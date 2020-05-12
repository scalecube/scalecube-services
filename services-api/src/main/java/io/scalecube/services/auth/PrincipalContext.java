package io.scalecube.services.auth;

@FunctionalInterface
public interface PrincipalContext {

  <T> T get();
}
