package io.scalecube.services.sut.typed;

import java.util.StringJoiner;

public class Rectangle extends Shape {

  private double width;
  private double height;

  public Rectangle() {}

  public Rectangle(double width, double height) {
    super("R");
    this.width = width;
    this.height = height;
  }

  public double width() {
    return width;
  }

  public double height() {
    return height;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Rectangle.class.getSimpleName() + "[", "]")
        .add("width=" + width)
        .add("height=" + height)
        .toString();
  }
}
