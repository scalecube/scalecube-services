package io.scalecube.services.examples.auth;

import java.util.Map;

public record PrincipalProfile(String endpoint, String serviceRole, Map<String, String> headers) {}
