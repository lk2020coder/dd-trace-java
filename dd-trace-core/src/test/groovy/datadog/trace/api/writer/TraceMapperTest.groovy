package datadog.trace.api.writer

import datadog.trace.common.writer.ddagent.TraceMapper
import datadog.trace.common.writer.ddagent.TraceMapperV0_5
import datadog.trace.core.DDSpan
import datadog.trace.core.SpanFactory
import datadog.trace.core.serialization.msgpack.ByteBufferConsumer
import datadog.trace.core.serialization.msgpack.Packer
import datadog.trace.util.test.DDSpecification
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker

import java.nio.ByteBuffer

class TraceMapperTest extends DDSpecification {

  def "test trace mapper v0.5" () {
    when:
    TraceMapper traceMapper = new TraceMapperV0_5()
    List<DDSpan> spans = trace
    CapturingByteBufferConsumer sink = new CapturingByteBufferConsumer()
    Packer packer = new Packer(sink, ByteBuffer.allocate(1024))
    packer.format(trace, traceMapper)
    packer.flush()

    then:
    sink.captured != null
    ByteBuffer dictionaryBytes = traceMapper.getDictionary()

    MessageUnpacker dictionaryUnpacker = MessagePack.newDefaultUnpacker(dictionaryBytes)
    int dictionaryLength = dictionaryUnpacker.unpackArrayHeader()
    String[] dictionary = new String[dictionaryLength]
    for (int i = 0; i < dictionary.length; ++i) {
      dictionary[i] = dictionaryUnpacker.unpackString()
    }
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(sink.captured)
    1 == unpacker.unpackArrayHeader()
    int spanCount = unpacker.unpackArrayHeader()
    spans.size() == spanCount
    for (int i = 0; i < spanCount; ++i) {
      int arrayLength = unpacker.unpackArrayHeader()
      arrayLength == 11
      if (!unpacker.tryUnpackNil()) {
        String serviceName =  dictionary[unpacker.unpackInt()]
        serviceName == "my-service"
      }
      assert unpacker.tryUnpackNil() // operation name null
      if (!unpacker.tryUnpackNil()) {
        String resourceName = dictionary[unpacker.unpackInt()]
        resourceName != null
      }
      long traceId = unpacker.unpackLong()
      traceId == 1
      unpacker.unpackLong()
      unpacker.unpackLong()
      unpacker.unpackLong()
      unpacker.unpackLong()
      unpacker.unpackInt()
      int metaHeader = unpacker.unpackMapHeader()
      for (int j = 0; j < metaHeader; ++j) {
        if (!unpacker.tryUnpackNil()) {
          String key = dictionary[unpacker.unpackInt()]
          key != null
        }
        if (!unpacker.tryUnpackNil()) {
          String value = dictionary[unpacker.unpackInt()]
          value != null
        }
      }
      int metricsHeader = unpacker.unpackMapHeader()
      for (int j = 0; j < metricsHeader; ++j) {
        if (!unpacker.tryUnpackNil()) {
          String key = dictionary[unpacker.unpackInt()]
          key != null
        }
        unpacker.skipValue()
      }
      if (!unpacker.tryUnpackNil()) {
        String type = dictionary[unpacker.unpackInt()]
        type != null
      }
    }

    where:
    trace << [
      [SpanFactory.newSpanOf(1L)
         .setOperationName(null)
         .setTag("service.name", "my-service")
          .setTag("elasticsearch.version", "7.0")
         .setBaggageItem("baggage", "item")]
    ]
  }

  static class CapturingByteBufferConsumer implements ByteBufferConsumer {

    ByteBuffer captured

    @Override
    void accept(int messageCount, ByteBuffer buffer) {
      captured = buffer
    }
  }
}
