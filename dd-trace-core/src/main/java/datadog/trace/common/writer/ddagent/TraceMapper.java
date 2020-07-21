package datadog.trace.common.writer.ddagent;

import datadog.trace.core.DDSpan;
import datadog.trace.core.serialization.msgpack.Mapper;
import java.nio.ByteBuffer;
import java.util.List;

public interface TraceMapper extends Mapper<List<DDSpan>> {
  ByteBuffer getDictionary();

  void reset();
}
