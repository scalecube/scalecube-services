package io.scalecube.services.gateway.sut.typed;

import java.util.StringJoiner;

public class Square extends Shape {

  private double side;

  public Square() {}

  public Square(double side) {
    super("S");
    this.side = side;
  }

  public double side() {
    return side;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Square.class.getSimpleName() + "[", "]")
        .add("side=" + side)
        .toString();
  }
}
