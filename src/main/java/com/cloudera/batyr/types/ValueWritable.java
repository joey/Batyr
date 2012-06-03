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
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, typeCode.getCode());
    switch (typeCode) {
      case BOOLEAN:
        out.writeBoolean(getBooleanValue());
        break;
      case BYTE:
        out.writeByte(getByteValue());
        break;
      case CHARACTER:
        out.writeChar(getCharacterValue());
        break;
      case SHORT:
        out.writeShort(getShortValue());
        break;
      case INTEGER:
        WritableUtils.writeVInt(out, getIntegerValue());
        break;
      case LONG:
        WritableUtils.writeVLong(out, getLongValue());
        break;
      case FLOAT:
        out.writeFloat(getFloatValue());
        break;
      case DOUBLE:
        out.writeDouble(getDoubleValue());
        break;
      case STRING:
        String string = getStringValue();
        ByteBuffer buf = Text.encode(string);
        WritableUtils.writeVInt(out, buf.limit());
        out.write(buf.array(), 0, buf.limit());
        break;
      case LIST:
        List list = getListValue();
        WritableUtils.writeVInt(out, list.size());
        for (Object v : list) {
          new ValueWritable(v).write(out);
        }
        break;
      case SET:
        Set set = getSetValue();
        WritableUtils.writeVInt(out, set.size());
        for (Object v : set) {
          new ValueWritable(v).write(out);
        }
        break;
      case MAP:
        Map<Object, Object> map = getMapValue();
        WritableUtils.writeVInt(out, map.size());
        for (Entry<Object, Object> entry : map.entrySet()) {
          new ValueWritable(entry.getKey()).write(out);
          new ValueWritable(entry.getValue()).write(out);
        }
        break;
      case SERIALIZABLE:
        Serializable serializable = getSerializableValue();
        final DataOutput finalOut = out;
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(out instanceof OutputStream ? (OutputStream) out : new OutputStream() {

          @Override
          public void write(int b) throws IOException {
            finalOut.write(b);
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
  public void readFields(DataInput in) throws IOException {
    typeCode = TypeCode.fromCode(WritableUtils.readVInt(in));
    Object v = null;
    switch (typeCode) {
      case BOOLEAN:
        v = Boolean.valueOf(in.readBoolean());
        break;
      case BYTE:
        v = Byte.valueOf(in.readByte());
        break;
      case CHARACTER:
        v = Character.valueOf(in.readChar());
        break;
      case SHORT:
        v = Short.valueOf(in.readShort());
        break;
      case INTEGER:
        v = Integer.valueOf(WritableUtils.readVInt(in));
        break;
      case LONG:
        v = Long.valueOf(WritableUtils.readVLong(in));
        break;
      case FLOAT:
        v = Float.valueOf(in.readFloat());
        break;
      case DOUBLE:
        v = Double.valueOf(in.readDouble());
        break;
      case STRING:
        int length = WritableUtils.readVInt(in);
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        v = Text.decode(bytes, 0, length);
        break;
      case LIST:
        length = WritableUtils.readVInt(in);
        List list = new ArrayList(length);
        ValueWritable valueWritable = new ValueWritable();
        while (length-- > 0) {
          valueWritable.readFields(in);
          list.add(valueWritable.getValue());
        }
        v = list;
        break;
      case SET:
        length = WritableUtils.readVInt(in);
        Set set = new HashSet(length);
        valueWritable = new ValueWritable();
        while (length-- > 0) {
          valueWritable.readFields(in);
          set.add(valueWritable.getValue());
        }
        v = set;
        break;
      case MAP:
        length = WritableUtils.readVInt(in);
        Map map = new HashMap(length);
        ValueWritable mapKey = new ValueWritable();
        ValueWritable mapValue = new ValueWritable();
        while (length-- > 0) {
          mapKey.readFields(in);
          mapValue.readFields(in);
          map.put(mapKey.getValue(), mapValue.getValue());
        }
        v = map;
        break;
      case SERIALIZABLE:
        final DataInput finalIn = in;
        ObjectInputStream objectInputStream = new ObjectInputStream(in instanceof InputStream ? (InputStream) in : new InputStream() {

          @Override
          public int read() throws IOException {
            return finalIn.readByte();
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
