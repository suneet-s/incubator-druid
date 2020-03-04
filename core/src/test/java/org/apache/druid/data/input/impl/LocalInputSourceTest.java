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
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocalInputSourceTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
  public void testFileIteratorWithEmptyFilesIteratingNonEmptyFilesOnly() throws IOException
  {
    final File dir = temporaryFolder.newFolder();
    prepareFiles(dir, 10, "test");
    prepareFiles(dir, 10, "");
    final LocalInputSource inputSource = new LocalInputSource(dir, "*");
    List<File> iteratedFiles = Lists.newArrayList(inputSource.getFileIterator());
    Assert.assertTrue(iteratedFiles.stream().allMatch(file -> file.length() > 0));
  }

  private Set<File> prepareFiles(File dir, int numFiles, String content) throws IOException
  {
    final Set<File> files = new HashSet<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = new File(dir, StringUtils.format("file-%d", i));
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write(content);
      }
      files.add(file);
    }
    return files;
  }
}
