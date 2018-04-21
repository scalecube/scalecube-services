
public class MyPojo {
  
  private int age;
  String name;
  
  @Deprecated // only for deserialization needs.
  public MyPojo() {}
  
  public MyPojo(String name,int age) {
    this.name = name;
    this.age = age;
  }
  
  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }
}
