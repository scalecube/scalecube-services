package io.scalecube.services.streaming;

import io.scalecube.services.Microservices;
import io.scalecube.transport.Message;

import rx.Observable;

public class Test {

  public static void main(String[] args) {

    Microservices ms = Microservices.builder().build();
   
    Observable.create(call->{
        ms.cluster().listenGossips().subscribe(onNext->{
          call.onNext(onNext);
        });
       
    }).subscribe(onNext->{
      System.out.println(onNext);
    });

    Microservices.builder().seeds(ms.cluster().address()).build().cluster().spreadGossip(Message.builder().build());;
    System.out.println("");
  }

}


