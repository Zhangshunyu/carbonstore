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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

/**
 * Schema writer writes schema file to the given path.
 */
public class CarbonSchemaWriter {

  public static void writeSchema(String tablePath, Schema schema, Map<String, String> tblProperties,
      Configuration configuration) throws IOException, InvalidLoadOptionException {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    CarbonWriterBuilder builder = new CarbonWriterBuilder();
    builder.withTableProperties(tblProperties).outputPath(tablePath);
    builder.withHadoopConf(configuration);
    CarbonLoadModel loadModel = builder.buildLoadModel(schema);
    CarbonTable table = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    // TODO don't hardcode
    table.getTableInfo().getFactTable().getTableProperties().put("streaming", "true");
    persistSchemaFile(table, tablePath);
  }

  public static void writeSchema(String tablePath, String jsonSchema, Configuration configuration)
      throws IOException, InvalidLoadOptionException {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    Map<String, String> tblProperties = new HashMap<>();
    Schema schema = convertToSchemaFromJSON(jsonSchema, tblProperties);
    writeSchema(tablePath, schema, tblProperties, configuration);
  }

  public static Schema convertToSchemaFromJSON(String jsonSchema,
      Map<String, String> tblProperties) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> jsonNodeMap =
        objectMapper.readValue(jsonSchema, new TypeReference<Map<String, Object>>() {
        });
    List<Field> fields = new ArrayList<>();
    for (Map.Entry<String, Object> entry : jsonNodeMap.entrySet()) {
      String fieldName = entry.getKey();
      if (entry.getValue() instanceof Map && fieldName.equalsIgnoreCase("tblproperties")) {
        Map<String, Object> properties = (Map) entry.getValue();
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
          tblProperties.put(prop.getKey(), prop.getValue().toString());
        }
      } else {
        fields.add(new Field(fieldName, convertDataType(entry.getValue().toString())));
      }
    }
    return new Schema(fields.toArray(new Field[0]));
  }

  private static DataType convertDataType(String dataType) {
    dataType = dataType.toUpperCase();
    if (dataType.equals(DataTypes.BOOLEAN.getName())) {
      return DataTypes.BOOLEAN;
    } else if (dataType.equals(DataTypes.STRING.getName())) {
      return DataTypes.STRING;
    } else if (dataType.equals(DataTypes.INT.getName())) {
      return DataTypes.INT;
    } else if (dataType.equals(DataTypes.SHORT.getName()) || dataType.equals("SMALLINT")) {
      return DataTypes.SHORT;
    } else if (dataType.equals(DataTypes.LONG.getName()) || dataType.equals("BIGINT")) {
      return DataTypes.LONG;
    } else if (dataType.equals(DataTypes.DOUBLE.getName())) {
      return DataTypes.DOUBLE;
      // TODo fix me
    } else if (dataType.startsWith("DECIMAL")) {
      return DataTypes.createDecimalType(10, 0);
    } else if (dataType.equals(DataTypes.DATE.getName())) {
      return DataTypes.DATE;
    } else if (dataType.equals(DataTypes.TIMESTAMP.getName())) {
      return DataTypes.TIMESTAMP;
    } else if (dataType.equals(DataTypes.VARCHAR.getName())) {
      return DataTypes.VARCHAR;
    } else if (dataType.equals(DataTypes.FLOAT.getName())) {
      return DataTypes.FLOAT;
    } else if (dataType.equals(DataTypes.BYTE.getName()) || dataType.equals("TINYINT")) {
      return DataTypes.BYTE;
    } else {
      throw new UnsupportedOperationException(
          "Provided datatype " + dataType + " is not supported");
    }
  }

  /**
   * Save the schema of the {@param table} to {@param persistFilePath}
   *
   * @param table     table object containing schema
   * @param tablePath absolute file path with file name
   */
  private static void persistSchemaFile(CarbonTable table, String tablePath) throws IOException {
    String persistFilePath = CarbonTablePath.getSchemaFilePath(tablePath);
    TableInfo tableInfo = table.getTableInfo();
    String schemaMetadataPath = CarbonTablePath.getMetadataPath(tablePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo = schemaConverter
        .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);
    FileFactory.mkdirs(schemaMetadataPath, FileFactory.getFileType(schemaMetadataPath));
    ThriftWriter thriftWriter = new ThriftWriter(persistFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
  }
}
