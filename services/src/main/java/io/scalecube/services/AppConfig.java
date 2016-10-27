package io.scalecube.services;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.scalecube.services.routing.RoundRubinServiceRouter;

public class AppConfig {

  static Config conf = ConfigFactory.load();

  public static String consulAddress() {
    return conf.getString("consul.address");
  }

  public static Class<?> routing(String forName) {
    try {
      return Class.forName(conf.getString(forName + "." + "routing"));
    } catch (ClassNotFoundException e) {
      return RoundRubinServiceRouter.class;
    }
  }

  public static String[] tags(String forName) {
    conf.getStringList(forName + "." + "routing");
    return null;
  }

}
