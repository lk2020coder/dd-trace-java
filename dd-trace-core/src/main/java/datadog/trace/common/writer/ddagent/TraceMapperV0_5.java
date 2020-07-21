package datadog.trace.common.writer.ddagent;

import static datadog.trace.core.serialization.msgpack.EncodingCachingStrategies.NO_CACHING;
import static datadog.trace.core.serialization.msgpack.Util.writeLongAsString;

import datadog.trace.bootstrap.instrumentation.api.UTF8BytesString;
import datadog.trace.core.DDSpan;
import datadog.trace.core.StringTables;
import datadog.trace.core.serialization.msgpack.ByteBufferConsumer;
import datadog.trace.core.serialization.msgpack.Mapper;
import datadog.trace.core.serialization.msgpack.Packer;
import datadog.trace.core.serialization.msgpack.Writable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TraceMapperV0_5 implements TraceMapper {

  private static final class DictionaryFull extends BufferOverflowException {
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  private static final DictionaryFull DICTIONARY_FULL = new DictionaryFull();

  // TODO probably need to modify Packer to make getting the dictionary
  //  a bit less hacky
  private final ByteBuffer[] dictionary = new ByteBuffer[1];
  private final Packer dictionaryWriter =
      new Packer(
          new ByteBufferConsumer() {
            @Override
            public void accept(int messageCount, ByteBuffer buffer) {
              dictionary[0] = buffer;
            }
          },
          ByteBuffer.allocate(2 << 20),
          true);
  private final DictionaryMapper dictionaryMapper = new DictionaryMapper();

  private int code = 1;
  // TODO use a primitive collection e.g. fastutil ObjectIntHashMap
  private final Map<Object, Integer> encoding = new HashMap<>();

  public TraceMapperV0_5() {
    reset();
  }

  @Override
  public void map(List<DDSpan> trace, Writable writable) {
    if (dictionary[0] != null) {
      // signal the need for a flush because the string table filled up
      // faster than the message content
      throw DICTIONARY_FULL;
    }
    writable.startArray(trace.size());
    for (DDSpan span : trace) {
      writable.startArray(12);
      /* 1  */
      writable.writeInt(getDictionaryCode(span.getServiceName()));
      /* 2  */
      writable.writeInt(getDictionaryCode(span.getOperationName()));
      /* 3  */
      writable.writeInt(getDictionaryCode(span.getResourceName()));
      /* 4  */
      writable.writeLong(span.getTraceId().toLong());
      /* 5  */
      writable.writeLong(span.getSpanId().toLong());
      /* 6  */
      writable.writeLong(span.getParentId().toLong());
      /* 7  */
      writable.writeLong(span.getStartTime());
      /* 8  */
      writable.writeLong(span.getDurationNano());
      /* 9  */
      writable.writeInt(span.getError());
      /* 10  */
      Map<String, Object> tags = span.context().getTags();
      Map<String, String> baggage = span.context().getBaggageItems();
      writable.startMap(baggage.size() + tags.size());
      for (Map.Entry<String, String> entry : baggage.entrySet()) {
        if (!tags.containsKey(entry.getKey())) {
          writable.writeInt(getDictionaryCode(entry.getKey()));
          writable.writeInt(getDictionaryCode(entry.getValue()));
        }
      }
      for (Map.Entry<String, Object> entry : tags.entrySet()) {
        writable.writeInt(getDictionaryCode(entry.getKey()));
        writable.writeInt(getDictionaryCode(entry.getValue()));
      }
      /* 11  */
      writable.startMap(span.getMetrics().size());
      for (Map.Entry<String, Number> entry : span.getMetrics().entrySet()) {
        writable.writeInt(getDictionaryCode(entry.getKey()));
        writable.writeObject(entry.getValue(), NO_CACHING);
      }
      /* 12 */
      writable.writeInt(getDictionaryCode(span.getType()));
    }
  }

  private int getDictionaryCode(Object value) {
    if (null == value) {
      return 0;
    }
    Integer encoded = encoding.get(value);
    if (null == encoded) {
      dictionaryWriter.format(value, dictionaryMapper);
      int snapshot = code++;
      encoding.put(value, snapshot);
      return snapshot;
    }
    return encoded;
  }

  @Override
  public ByteBuffer getDictionary() {
    if (dictionary[0] == null) {
      dictionaryWriter.flush();
    }
    return dictionary[0];
  }

  @Override
  public void reset() {
    dictionaryWriter.reset();
    dictionary[0] = null;
    // null strings are always encoded as 0 for ease of decoding,
    // so null needs to be the first element in the dictionary
    dictionaryWriter.format(null, dictionaryMapper);
    code = 1;
    encoding.clear();
  }

  private static class DictionaryMapper implements Mapper<Object> {

    private final byte[] numberByteArray = new byte[20]; // this is max long digits and sign

    @Override
    public void map(Object data, Writable packer) {
      if (null == data) {
        packer.writeNull();
      } else if (data instanceof UTF8BytesString) {
        packer.writeUTF8(((UTF8BytesString) data).getUtf8Bytes());
      } else if (data instanceof Long || data instanceof Integer) {
        writeLongAsString(((Number) data).longValue(), packer, numberByteArray);
      } else {
        String string = String.valueOf(data);
        byte[] utf8 = StringTables.getKeyBytesUTF8(string);
        if (null == utf8) {
          utf8 = StringTables.getTagBytesUTF8(string);
          if (null == utf8) {
            packer.writeString(string, NO_CACHING);
            return;
          }
        }
        packer.writeUTF8(utf8);
      }
    }
  }
}
