/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonSchemaWriterTest extends TestCase {
  String path = "./testWriteFiles";

  @Before
  public void setUp() throws IOException, InvalidLoadOptionException {

  }

  @Test
  public void testReadSchemaFromDataFile() throws IOException, InvalidLoadOptionException {
    Map<String, String> map = new HashMap<>();
    Schema schema = CarbonSchemaWriter.convertToSchemaFromJSON(
        "{\"ID\":\"int\",\"name\":\"string\",\"salary\":\"double\",\"timestamp\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\"}}",
        map);
    schema.getProperties().put(CarbonCommonConstants.PRIMARY_KEY_COLUMNS, "ID");
    CarbonSchemaWriter.writeSchema(path, schema, FileFactory.getConfiguration());


    Schema schema1 =
        CarbonSchemaReader.readSchemaInSchemaFile(CarbonTablePath.getSchemaFilePath(path));
    System.out.println(schema1);
  }


  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }
}
