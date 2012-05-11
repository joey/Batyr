package com.cloudera.batyr.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

public class ValueWritable<V> implements Writable {

  private static Logger LOG = Logger.getLogger(ValueWritable.class);
  private static TypeCodes codes = TypeCodes.get();
  int typeCode;
  V value;

  public ValueWritable() {
  }

  public ValueWritable(V value) {
    setValue(value);
  }

  @Override
  public String toString() {
    return value.toString();
  }

  public final void setValue(V value) {
    Class<?> clazz = (value == null ? null : value.getClass());
    if (codes.containsKey(clazz) == false) {
      throw new IllegalArgumentException("Class " + clazz + " is not supported by ValuesWritable");
    }
    this.typeCode = codes.get(clazz);
    this.value = value;
  }

  public V getValue() {
    return value;
  }

  public Boolean getBooleanValue() {
    return (Boolean) value;
  }

  public Byte getByteValue() {
    return (Byte) value;
  }

  public Character getCharacterValue() {
    return (Character) value;
  }

  public Short getShortValue() {
    return (Short) value;
  }

  public Integer getIntegerValue() {
    return (Integer) value;
  }

  public Long getLongValue() {
    return (Long) value;
  }

  public Float getFloatValue() {
    return (Float) value;
  }

  public Double getDoubleValue() {
    return (Double) value;
  }

  public String getStringValue() {
    return (String) value;
  }

  public List getListValue() {
    return (List) value;
  }

  public Set getSetValue() {
    return (Set) value;
  }

  public Map getMapValue() {
    return (Map) value;
  }

  public Serializable getSerializableValue() {
    return (Serializable) value;
  }

//    map.put(Boolean.class, 0);
//    map.put(Byte.class, 1);
//    map.put(Character.class, 2);
//    map.put(Short.class, 3);
//    map.put(Integer.class, 4);
//    map.put(Long.class, 5);
//    map.put(Float.class, 6);
//    map.put(Double.class, 7);
//    map.put(String.class, 8);
//
//    map.put(List.class, 9);
//    map.put(Set.class, 10);
//    map.put(Map.class, 11);
//    map.put(Serializable.class, 12);
//
//    map.put(Null.class, 13);
  @Override
  public void write(DataOutput d) throws IOException {
    WritableUtils.writeVInt(d, typeCode);
    switch (typeCode) {
      case 0:
        d.writeBoolean(getBooleanValue());
        break;
      case 1:
        d.writeByte(getByteValue());
        break;
      case 2:
        d.writeChar(getCharacterValue());
        break;
      case 3:
        d.writeShort(getShortValue());
        break;
      case 4:
        WritableUtils.writeVInt(d, getIntegerValue());
        break;
      case 5:
        WritableUtils.writeVLong(d, getLongValue());
        break;
      case 6:
        d.writeFloat(getFloatValue());
        break;
      case 7:
        d.writeDouble(getDoubleValue());
        break;
      case 8:
        String string = getStringValue();
        ByteBuffer buf = Text.encode(string);
        WritableUtils.writeVInt(d, buf.limit());
        d.write(buf.array(), 0, buf.limit());
        break;
      case 9:
        List list = getListValue();
        WritableUtils.writeVInt(d, list.size());
        for (Object v : list) {
          new ValueWritable(v).write(d);
        }
        break;
      case 10:
        Set set = getSetValue();
        WritableUtils.writeVInt(d, set.size());
        for (Object v : set) {
          new ValueWritable(v).write(d);
        }
        break;
      case 11:
        Map<Object, Object> map = getMapValue();
        WritableUtils.writeVInt(d, map.size());
        for (Entry<Object, Object> entry : map.entrySet()) {
          new ValueWritable(entry.getKey()).write(d);
          new ValueWritable(entry.getValue()).write(d);
        }
        break;
      case 12:
        Serializable serializable = getSerializableValue();
        final DataOutput finalD = d;
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(d instanceof OutputStream ? (OutputStream) d : new OutputStream() {

          @Override
          public void write(int b) throws IOException {
            finalD.write(b);
          }
        });
        objectOutputStream.writeObject(serializable);
        objectOutputStream.flush();
        break;
      case 13:
        break;
      default:
        throw new IOException("Invalid type code " + typeCode);
    }
  }

  @Override
  public void readFields(DataInput di) throws IOException {
    typeCode = WritableUtils.readVInt(di);
    Object v = null;
    switch (typeCode) {
      case 0:
        v = Boolean.valueOf(di.readBoolean());
        break;
      case 1:
        v = Byte.valueOf(di.readByte());
        break;
      case 2:
        v = Character.valueOf(di.readChar());
        break;
      case 3:
        v = Short.valueOf(di.readShort());
        break;
      case 4:
        v = Integer.valueOf(WritableUtils.readVInt(di));
        break;
      case 5:
        v = Long.valueOf(WritableUtils.readVLong(di));
        break;
      case 6:
        v = Float.valueOf(di.readFloat());
        break;
      case 7:
        v = Double.valueOf(di.readDouble());
        break;
      case 8:
        int length = WritableUtils.readVInt(di);
        byte[] bytes = new byte[length];
        di.readFully(bytes, 0, length);
        v = Text.decode(bytes, 0, length);
        break;
      case 9:
        length = WritableUtils.readVInt(di);
        List list = new ArrayList(length);
        ValueWritable valueWritable = new ValueWritable();
        while (length-- > 0) {
          valueWritable.readFields(di);
          list.add(valueWritable.getValue());
        }
        break;
      case 10:
        length = WritableUtils.readVInt(di);
        Set set = new HashSet(length);
        valueWritable = new ValueWritable();
        while (length-- > 0) {
          valueWritable.readFields(di);
          set.add(valueWritable.getValue());
        }
        break;
      case 11:
        length = WritableUtils.readVInt(di);
        Map map = new HashMap(length);
        ValueWritable mapKey = new ValueWritable();
        ValueWritable mapValue = new ValueWritable();
        while (length-- > 0) {
          mapKey.readFields(di);
          mapValue.readFields(di);
          map.put(mapKey.getValue(), mapValue.getValue());
        }

        break;
      case 12:
        final DataInput finalDi = di;
        ObjectInputStream objectInputStream = new ObjectInputStream(di instanceof InputStream ? (InputStream) di : new InputStream() {

          @Override
          public int read() throws IOException {
            return finalDi.readByte();
          }
        });
        try {
          v = objectInputStream.readObject();
        } catch (ClassNotFoundException ex) {
          LOG.error("Can't deserialize Serializable value because the associated class is missing", ex);
          throw new IOException("Failed to deserialize value", ex);
        }
        break;
      case 13:
        v = null;
        break;
      default:
        throw new IOException("Invalid type code " + typeCode);
    }
    value = (V) v;
  }
}
