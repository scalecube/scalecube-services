package io.scalecube.services.sut.typed;

import java.util.StringJoiner;

public class Square extends Rectangle {

  private double side;

  public Square() {}

  public Square(double side) {
    super(side, side);
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
