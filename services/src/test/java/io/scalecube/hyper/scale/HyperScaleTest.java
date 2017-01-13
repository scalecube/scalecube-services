package io.scalecube.hyper.scale;

import io.scalecube.services.Microservices;

import org.junit.Ignore;
import org.junit.Test;

public class HyperScaleTest {

  @Test
  public void test_hyper_scale () {
    Microservices seed = Microservices.builder().build();
    
    for(int i = 0 ; i< 2000; i=i+2){
      Microservices.builder().port(4010+ i).seeds(seed.cluster().address()).build();
    }
  }
}
