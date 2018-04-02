package org.apache.carbondata.mv.tool.reader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.mv.tool.constants.MVAdvisiorConstants;
import org.apache.carbondata.mv.tool.vo.ColStats;
import org.apache.carbondata.mv.tool.vo.TableDetailsVo;
import org.apache.carbondata.mv.tool.vo.TblStatsVo;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class JsonFileReader {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(JsonFileReader.class.getName());

  public static void main(String[] args) throws FileNotFoundException {
    String filePath = "D:/mvinput/querylog.sql";
    List<String> strings = readQuerySql(filePath);
    System.out.println();
  }

  public static List<TableDetailsVo> readTableDetails(String filePath)
      throws FileNotFoundException {
    JsonParser parser = new JsonParser();
    List<TableDetailsVo> tblDetailsVo = new ArrayList<>();
    try(FileReader reader = new FileReader(filePath)) {
      JsonElement parse = parser.parse(new JsonReader(reader));
      Iterator<JsonElement> iterator = parse.getAsJsonArray().iterator();
      while (iterator.hasNext()) {
        JsonElement next = iterator.next();
        JsonObject object = ((JsonObject) next);
        if (null == object.get(MVAdvisiorConstants.JSON_TABLENAME) || null == object
            .get(MVAdvisiorConstants.JSON_ISFACTTABLE) || null == object
            .get(MVAdvisiorConstants.JSON_SCHEMA) || null == object
            .get(MVAdvisiorConstants.JSON_TABLESTATS) || null == object
            .get(MVAdvisiorConstants.JSON_COLSTATS)) {
          throw new RuntimeException("Invalid JSON File");
        }
        Iterator<JsonElement> colstats =
            object.get(MVAdvisiorConstants.JSON_COLSTATS).getAsJsonArray().iterator();
        Map<String, ColStats> colStatsMap = new HashMap<>();
        while (colstats.hasNext()) {
          JsonObject next1 = (JsonObject) colstats.next();
          if (null == next1.get(MVAdvisiorConstants.JSON_COLNAME) || null == next1
              .get(MVAdvisiorConstants.JSON_NDV)) {
            throw new RuntimeException("Invalid File columnName or ndv cannot be empty");
          }
          String columName = next1.get(MVAdvisiorConstants.JSON_COLNAME).getAsString();
          ColStats stats = new ColStats(next1.get(MVAdvisiorConstants.JSON_NDV).getAsLong());
          if(null!= next1.get(MVAdvisiorConstants.JSON_MAXVALUE)) {
            stats.setMaxValue(next1.get(MVAdvisiorConstants.JSON_MAXVALUE).getAsString());
          }
          if(null != next1.get(MVAdvisiorConstants.JSON_MINVALUE)) {
            stats.setMinValue(next1.get(MVAdvisiorConstants.JSON_MINVALUE).getAsString());
          }
          if (null != next1.get(MVAdvisiorConstants.JSON_NULLCOUNT)) {
            stats.setNullCount(next1.get(MVAdvisiorConstants.JSON_NULLCOUNT).getAsInt());
          }
          if (null != next1.get(MVAdvisiorConstants.JSON_MAXLENGTH)) {
            stats.setMaxLength(next1.get(MVAdvisiorConstants.JSON_MAXLENGTH).getAsInt());
          }
          colStatsMap.put(columName, stats);
        }
        JsonObject tblStats =
            ((JsonObject) object.get(MVAdvisiorConstants.JSON_TABLESTATS).getAsJsonArray().get(0));
        TblStatsVo tblStatsVo =
            new TblStatsVo(tblStats.get(MVAdvisiorConstants.JSON_TABLESIZE).getAsLong(),
                tblStats.get(MVAdvisiorConstants.JSON_ROWCOUNT).getAsLong());
        TableDetailsVo tableDetailsVo =
            new TableDetailsVo(object.get(MVAdvisiorConstants.JSON_TABLENAME).getAsString(),
                object.get(MVAdvisiorConstants.JSON_ISFACTTABLE).getAsBoolean(),
                object.get(MVAdvisiorConstants.JSON_SCHEMA).getAsString(), tblStatsVo,
                colStatsMap);
        tblDetailsVo.add(tableDetailsVo);

      }
    } catch(IOException e) {
       LOGGER.error(e, "Problem while closing the reader");
    }
    return tblDetailsVo;
  }

  public static List<String> readQuerySql(String querySqlFile) throws FileNotFoundException {
    JsonParser parser = new JsonParser();
    FileReader reader = null;
    List<String> querySqls = new ArrayList<>();
    try {
      reader = new FileReader(querySqlFile);
      JsonElement parse = parser.parse(new JsonReader(reader));
      Iterator<JsonElement> iterator = parse.getAsJsonArray().iterator();
      while (iterator.hasNext()) {
        JsonElement next = iterator.next();
        JsonObject object = ((JsonObject) next);
        querySqls.add(object.get(MVAdvisiorConstants.JSON_SQL).getAsString());
      }
    } finally {
      if (null != reader) {
        try {
          reader.close();
        } catch (IOException e) {
          LOGGER.error(e, "Problem while closing the reader");
        }
      }
    }
    return querySqls;
  }
}
