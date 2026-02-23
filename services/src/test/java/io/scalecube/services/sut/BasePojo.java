package io.scalecube.services.sut;

public class BasePojo<T> {

  T obj;
  String format;

  public BasePojo object(T obj) {
    this.obj = obj;
    return this;
  }

  public BasePojo format(String format) {
    this.format = format;
    return this;
  }
}
