package org.apache.druid.server.coordinator.duty.compaction.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactionSegmentIterator;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * This policy searches segments for compaction from the oldest to the newest one.
 */
public class NewestSegmentLastPolicy implements CompactionSegmentSearchPolicy
{
  private final ObjectMapper objectMapper;

  public NewestSegmentLastPolicy(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  @Override
  public CompactionSegmentIterator reset(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, SegmentTimeline> dataSources,
      Map<String, List<Interval>> skipIntervals
  )
  {
    // TODO: test well
    return new NewestSegmentFirstLastIterator(objectMapper, compactionConfigs, dataSources, skipIntervals, Comparators.intervalsByStartThenEnd().reversed());
  }
}