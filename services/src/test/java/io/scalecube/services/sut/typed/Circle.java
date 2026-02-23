package io.scalecube.services.sut.typed;

import java.util.StringJoiner;

public class Circle extends Shape {

  private double radius;

  public Circle() {}

  public Circle(double radius) {
    super("C");
    this.radius = radius;
  }

  public double radius() {
    return radius;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Circle.class.getSimpleName() + "[", "]")
        .add("radius=" + radius)
        .toString();
  }
}
