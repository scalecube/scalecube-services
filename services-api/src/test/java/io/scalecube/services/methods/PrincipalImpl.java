package io.scalecube.services.methods;

import io.scalecube.services.auth.Principal;
import java.util.List;

public record PrincipalImpl(String role, List<String> permissions) implements Principal {}
