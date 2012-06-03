package com.cloudera.batyr.types;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

public class ValueWritable<V> implements Writable {

  private static Logger LOG = Logger.getLogger(ValueWritable.class);
  private static TypeCodes codes = TypeCodes.get();
  TypeCode typeCode;
  V value;
  public ValueWritable() {
  }

  public ValueWritable(V value) {
    setValue(value);
  }

  @Override
  public String toString() {
    return value == null ? "null" : value.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof ValueWritable) {
      ValueWritable other = (ValueWritable) obj;
      if (other.value == null) {
        return value == null;
      }
      return other.typeCode == typeCode && other.value.equals(value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 17 * hash + (this.typeCode != null ? this.typeCode.hashCode() : 0);
    hash = 17 * hash + (this.value != null ? this.value.hashCode() : 0);
    return hash;
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

  @Override
  public void write(DataOutput d) throws IOException {
    WritableUtils.writeVInt(d, typeCode.getCode());
    switch (typeCode) {
      case BOOLEAN:
        d.writeBoolean(getBooleanValue());
        break;
      case BYTE:
        d.writeByte(getByteValue());
        break;
      case CHARACTER:
        d.writeChar(getCharacterValue());
        break;
      case SHORT:
        d.writeShort(getShortValue());
        break;
      case INTEGER:
        WritableUtils.writeVInt(d, getIntegerValue());
        break;
      case LONG:
        WritableUtils.writeVLong(d, getLongValue());
        break;
      case FLOAT:
        d.writeFloat(getFloatValue());
        break;
      case DOUBLE:
        d.writeDouble(getDoubleValue());
        break;
      case STRING:
        String string = getStringValue();
        ByteBuffer buf = Text.encode(string);
        WritableUtils.writeVInt(d, buf.limit());
        d.write(buf.array(), 0, buf.limit());
        break;
      case LIST:
        List list = getListValue();
        WritableUtils.writeVInt(d, list.size());
        for (Object v : list) {
          new ValueWritable(v).write(d);
        }
        break;
      case SET:
        Set set = getSetValue();
        WritableUtils.writeVInt(d, set.size());
        for (Object v : set) {
          new ValueWritable(v).write(d);
        }
        break;
      case MAP:
        Map<Object, Object> map = getMapValue();
        WritableUtils.writeVInt(d, map.size());
        for (Entry<Object, Object> entry : map.entrySet()) {
          new ValueWritable(entry.getKey()).write(d);
          new ValueWritable(entry.getValue()).write(d);
        }
        break;
      case SERIALIZABLE:
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
      case NULL:
        break;
      default:
        throw new IOException("Invalid type code " + typeCode);
    }
  }

  @Override
  public void readFields(DataInput di) throws IOException {
    typeCode = TypeCode.fromCode(WritableUtils.readVInt(di));
    Object v = null;
    switch (typeCode) {
      case BOOLEAN:
        v = Boolean.valueOf(di.readBoolean());
        break;
      case BYTE:
        v = Byte.valueOf(di.readByte());
        break;
      case CHARACTER:
        v = Character.valueOf(di.readChar());
        break;
      case SHORT:
        v = Short.valueOf(di.readShort());
        break;
      case INTEGER:
        v = Integer.valueOf(WritableUtils.readVInt(di));
        break;
      case LONG:
        v = Long.valueOf(WritableUtils.readVLong(di));
        break;
      case FLOAT:
        v = Float.valueOf(di.readFloat());
        break;
      case DOUBLE:
        v = Double.valueOf(di.readDouble());
        break;
      case STRING:
        int length = WritableUtils.readVInt(di);
        byte[] bytes = new byte[length];
        di.readFully(bytes, 0, length);
        v = Text.decode(bytes, 0, length);
        break;
      case LIST:
        length = WritableUtils.readVInt(di);
        List list = new ArrayList(length);
        ValueWritable valueWritable = new ValueWritable();
        while (length-- > 0) {
          valueWritable.readFields(di);
          list.add(valueWritable.getValue());
        }
        v = list;
        break;
      case SET:
        length = WritableUtils.readVInt(di);
        Set set = new HashSet(length);
        valueWritable = new ValueWritable();
        while (length-- > 0) {
          valueWritable.readFields(di);
          set.add(valueWritable.getValue());
        }
        v = set;
        break;
      case MAP:
        length = WritableUtils.readVInt(di);
        Map map = new HashMap(length);
        ValueWritable mapKey = new ValueWritable();
        ValueWritable mapValue = new ValueWritable();
        while (length-- > 0) {
          mapKey.readFields(di);
          mapValue.readFields(di);
          map.put(mapKey.getValue(), mapValue.getValue());
        }
        v = map;
        break;
      case SERIALIZABLE:
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
      case NULL:
        v = null;
        break;
      default:
        throw new IOException("Invalid type code " + typeCode);
    }
    value = (V) v;
  }

}
