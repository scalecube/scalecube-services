package io.scalecube.services.gateway.sut.typed;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
  @Type(value = Circle.class, name = "C"),
  @Type(value = Rectangle.class, name = "R"),
  @Type(value = Square.class, name = "S"),
})
public abstract class Shape {

  protected String type;

  public Shape() {}

  public Shape(String type) {
    this.type = type;
  }
}
