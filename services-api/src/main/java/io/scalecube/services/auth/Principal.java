package io.scalecube.services.auth;

import java.util.List;

public interface Principal {

  List<String> permissions();

  boolean hasPermission(String permission);
}
