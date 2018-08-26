package io.scalecube.gateway.websocket.message;

public interface TestInputs {
  Long SID = 42L;
  Integer I = 423;
  Integer SIG = 422;
  String Q = "/test/test";

  String NO_DATA =
    "{"
      + " \"q\":\""
      + Q
      + "\","
      + " \"sid\":"
      + SID
      + ","
      + " \"sig\":"
      + SIG
      + ","
      + " \"i\":"
      + I
      + "}";

  String STRING_DATA_PATTERN_Q_SIG_SID_D =
    "{" + "\"q\":\"%s\"," + "\"sig\":%d," + "\"sid\":%d," + "\"d\":%s" + "}";

  String STRING_DATA_PATTERN_D_SIG_SID_Q =
    "{" + "\"d\": %s," + "\"sig\":%d," + "\"sid\": %d," + "\"q\":\"%s\"" + "}";

  class Entity {
    private String text;
    private Integer number;
    private Boolean check;

    Entity() {}

    public Entity(String text, Integer number, Boolean check) {
      this.text = text;
      this.number = number;
      this.check = check;
    }

    public String text() {
      return text;
    }

    public Integer number() {
      return number;
    }

    public Boolean check() {
      return check;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Entity entity = (Entity) o;

      if (text != null ? !text.equals(entity.text) : entity.text != null) {
        return false;
      }
      if (number != null ? !number.equals(entity.number) : entity.number != null) {
        return false;
      }
      return check != null ? check.equals(entity.check) : entity.check == null;
    }

    @Override
    public int hashCode() {
      int result = text != null ? text.hashCode() : 0;
      result = 31 * result + (number != null ? number.hashCode() : 0);
      result = 31 * result + (check != null ? check.hashCode() : 0);
      return result;
    }
  }
}
