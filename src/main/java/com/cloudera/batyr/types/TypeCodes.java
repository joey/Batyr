package com.cloudera.batyr.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class TypeCodes implements Map<Class<?>, Integer> {

  private static Logger LOG = Logger.getLogger(TypeCodes.class);
  private static final Map<Class<?>, Integer> map = new HashMap<Class<?>, Integer>();
  private static final TypeCodes codes = new TypeCodes();

  public static TypeCodes get() {
    return codes;
  }

  static {
    map.put(Boolean.class, 0);
    map.put(Byte.class, 1);
    map.put(Character.class, 2);
    map.put(Short.class, 3);
    map.put(Integer.class, 4);
    map.put(Long.class, 5);
    map.put(Float.class, 6);
    map.put(Double.class, 7);
    map.put(String.class, 8);

    map.put(List.class, 9);
    map.put(Set.class, 10);
    map.put(Map.class, 11);
    map.put(Serializable.class, 12);

    map.put(Null.class, 13);
  }

  private static class Null {
  }

  private TypeCodes() {
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    if (key == null) {
      return true;
    } else if (map.containsKey(key)) {
      return true;
    } else if (key instanceof Class) {
      Class clazz = (Class) key;
      if (List.class.isAssignableFrom(clazz)) {
        return true;
      } else if (Set.class.isAssignableFrom(clazz)) {
        return true;
      } else if (Map.class.isAssignableFrom(clazz)) {
        return true;
      } else if (Serializable.class.isAssignableFrom(clazz)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public Integer get(Object key) {
    if (key == null) {
      return map.get(Null.class);
    }
    Integer result = map.get(key);
    if (result != null) {
      return result;
    } else if (key instanceof Class) {
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
    } else {
      return null;
    }
  }

  @Override
  public Integer put(Class<?> key, Integer value) {
    throw new RuntimeException("TypeCodes are not modifiable at runtime");
  }

  @Override
  public Integer remove(Object key) {
    throw new RuntimeException("TypeCodes are not modifiable at runtime");
  }

  @Override
  public void putAll(Map<? extends Class<?>, ? extends Integer> m) {
    throw new RuntimeException("TypeCodes are not modifiable at runtime");
  }

  @Override
  public void clear() {
    throw new RuntimeException("TypeCodes are not modifiable at runtime");
  }

  @Override
  public Set<Class<?>> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<Integer> values() {
    return map.values();
  }

  @Override
  public Set<Entry<Class<?>, Integer>> entrySet() {
    return map.entrySet();
  }
}
