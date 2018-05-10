package io.scalecube.services;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import rx.Observable;

@Service
public interface Generator {
    @ServiceMethod
    Observable<Response> generate();

    class Response {
      public String data;

      public Response(String data) {
        this.data = data;
      }

      public String getData() {
        return data;
      }

      public void setData(String data) {
        this.data = data;
      }
    }
}