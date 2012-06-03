package com.cloudera.batyr.types;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class TypeCodes {

  private static Logger LOG = Logger.getLogger(TypeCodes.class);
  private static final Map<Class<?>, TypeCode> map = new HashMap<Class<?>, TypeCode>();
  private static final TypeCodes codes = new TypeCodes();
  public static TypeCodes get() {
    return codes;
  }

  static {
    map.put(Boolean.class, TypeCode.BOOLEAN);
    map.put(Byte.class, TypeCode.BYTE);
    map.put(Character.class, TypeCode.CHARACTER);
    map.put(Short.class, TypeCode.SHORT);
    map.put(Integer.class, TypeCode.INTEGER);
    map.put(Long.class, TypeCode.LONG);
    map.put(Float.class, TypeCode.FLOAT);
    map.put(Double.class, TypeCode.DOUBLE);
    map.put(String.class, TypeCode.STRING);

    map.put(List.class, TypeCode.LIST);
    map.put(Set.class, TypeCode.SET);
    map.put(Map.class, TypeCode.MAP);
    map.put(Serializable.class, TypeCode.SERIALIZABLE);

    // map.put(null, TypeCode.NULL);
  }

  private TypeCodes() {
  }

  public boolean containsKey(Class<?> key) {
    if (key == null) {
      return true;
    } else if (map.containsKey(key)) {
      return true;
    } else {
      if (List.class.isAssignableFrom(key)) {
        return true;
      } else if (Set.class.isAssignableFrom(key)) {
        return true;
      } else if (Map.class.isAssignableFrom(key)) {
        return true;
      } else if (Serializable.class.isAssignableFrom(key)) {
        return true;
      } else {
        return false;
      }
    }
  }

  public TypeCode get(Class<?> key) {
    if (key == null) {
      return TypeCode.NULL;
    }
    TypeCode result = map.get(key);
    if (result != null) {
      return result;
    } else {
      Class clazz = (Class) key;
      if (List.class.isAssignableFrom(clazz)) {
        return map.get(List.class);
      } else if (Set.class.isAssignableFrom(clazz)) {
        return map.get(Set.class);
      } else if (Map.class.isAssignableFrom(clazz)) {
        return map.get(Map.class);
      } else if (Serializable.class.isAssignableFrom(clazz)) {
        return map.get(Serializable.class);
      } else {
        return null;
      }
    }
  }

}
