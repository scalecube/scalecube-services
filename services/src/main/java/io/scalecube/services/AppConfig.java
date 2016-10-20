package io.scalecube.services;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {

  //Load our own config values from the default location,
  // application.conf
  static Config conf = ConfigFactory.load();
  
  
  public static String consulAddress(){
    return  conf.getString("consul.address");
  }
}
