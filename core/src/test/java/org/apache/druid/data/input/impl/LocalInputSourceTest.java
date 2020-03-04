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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputSource;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocalInputSourceTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final LocalInputSource source = new LocalInputSource(new File("myFile").getAbsoluteFile(), "myFilter");
    final byte[] json = mapper.writeValueAsBytes(source);
    final LocalInputSource fromJson = (LocalInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
  }

  @Test
  public void testFileIteratorWithEmptyFilesIteratingNonEmptyFilesOnly()
  {
    final Set<File> files = new HashSet<>(mockFiles(10, 5));
    files.addAll(mockFiles(10, 0));
    final LocalInputSource inputSource = new LocalInputSource(null, null, files);
    List<File> iteratedFiles = Lists.newArrayList(inputSource.getFileIterator());
    Assert.assertTrue(iteratedFiles.stream().allMatch(file -> file.length() > 0));
  }

  private static Set<File> mockFiles(int numFiles, long fileSize)
  {
    final Set<File> files = new HashSet<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = EasyMock.niceMock(File.class);
      EasyMock.expect(file.length()).andReturn(fileSize).anyTimes();
      EasyMock.replay(file);
      files.add(file);
    }
    return files;
  }
}
