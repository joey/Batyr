package com.cloudera.batyr.types;

public enum TypeCode {

  BOOLEAN(0),
  BYTE(1),
  CHARACTER(2),
  SHORT(3),
  INTEGER(4),
  LONG(5),
  FLOAT(6),
  DOUBLE(7),
  STRING(8),
  LIST(9),
  SET(10),
  MAP(11),
  SERIALIZABLE(12),
  NULL(13);
  private int code;
  private TypeCode(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static TypeCode fromCode(int code) {
    for (TypeCode tc : values()) {
      if (tc.getCode() == code) {
        return tc;
      }
    }
    throw new IllegalArgumentException("Invalid TypeCode: " + code);
  }

}
