/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.benchmark;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(value = 1)
public class MapGetBenchmark
{
  private static final int NUM_VALUES = 1_000_000;
  private static final Map<Object, IntList> HASH_MAP;
  private static final Object2ObjectMap<Object, IntList> OBJECT_FAST_MAP;
  private static final Long2ObjectMap<IntList> LONG_FAST_MAP;

  static {
    HASH_MAP = new HashMap<>();
    OBJECT_FAST_MAP = new Object2ObjectOpenHashMap<>();
    LONG_FAST_MAP = new Long2ObjectOpenHashMap<>();

    for (int i = 0; i < NUM_VALUES; i++) {
      long key = ThreadLocalRandom.current().nextLong();
      IntArrayList val = new IntArrayList();
      HASH_MAP.put(key, val);
      OBJECT_FAST_MAP.put(key, val);
      LONG_FAST_MAP.put(key, val);
    }
  }

  private Long searchKey = new Long(ThreadLocalRandom.current().nextLong());

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void hashMapGet(Blackhole blackhole)
  {
    IntList found = HASH_MAP.get(searchKey);
    blackhole.consume(found);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void objectFastMapGet(Blackhole blackhole)
  {
    IntList found = OBJECT_FAST_MAP.get(searchKey);
    blackhole.consume(found);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void longFastMapGet(Blackhole blackhole)
  {
    IntList found = LONG_FAST_MAP.get(searchKey.longValue());
    blackhole.consume(found);
  }
}
