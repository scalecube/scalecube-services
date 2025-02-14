package io.scalecube.services.examples.auth;

public record CompositeProfile(PrincipalProfile principalProfile, UserProfile userProfile) {}
