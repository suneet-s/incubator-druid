package org.apache.druid.server.coordinator.duty.compaction.policy;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An enum that describes the compaction search policy.
 */
public enum SearchPolicy
{
  /**
   * The default policy which returns the newest segment first.
   */
  NEWEST_FIRST,
  /**
   * A search policy which returns the newest segment last. This should reduce contention with new arriving data as
   * older data is compacted first.
   */
  NEWEST_LAST;

  public CompactionSegmentSearchPolicy getSearchPolicy(ObjectMapper objectMapper)
  {
    switch (this) {
      case NEWEST_FIRST:
        return new NewestSegmentFirstPolicy(objectMapper);
      case NEWEST_LAST:
        return new NewestSegmentLastPolicy(objectMapper);
      default:
        // TODO: is it better to silently return the old default? or crash and burn?
        return null;
    }
  }
}
