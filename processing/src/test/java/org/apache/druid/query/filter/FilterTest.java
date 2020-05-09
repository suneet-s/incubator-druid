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

package org.apache.druid.query.filter;

import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.util.Set;

public class FilterTest
{
  @Test
  public void testCustomEquals() throws NoSuchMethodException
  {
    Reflections reflections = new Reflections("org.apache.druid");
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(SubclassesMustOverrideEqualsAndHashCode.class);
    for (Class<?> clazz : classes) {
      if (clazz.isInterface()) {
        continue;
      }
      Method m = clazz.getMethod("hashCode");
      Assert.assertNotSame(clazz.getName() + " does not implment hashCode", Object.class, m.getDeclaringClass());
    }
  }
}
