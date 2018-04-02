package org.apache.carbondata.mv.tool.offlineadvision

import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.carbondata.mv.tool.manager.CommonSubexpressionManager
import org.apache.carbondata.mv.tool.preprocessor.QueryBatchPreprocessor
import org.apache.carbondata.mv.tool.vo.{ColStats, TableDetailsVo}

case class OfflineAdvisior(sparkSession: SparkSession) {

  def adviseMV(querySqls: Seq[String],
      tableDetails: Seq[TableDetailsVo],
      outputLoc: String): Unit = {
    createTablesAndCopyStats(tableDetails)
    val mvAdvisor = new MVAdvisor
    mvAdvisor.adviseMVs(sparkSession,
      DefaultQueryBatchPreprocessor,
      new DefaultSubExpressionManager(sparkSession,
        sparkSession.sessionState.conf,
        getTableProperty(tableDetails)),
      querySqls,
      outputLoc)
  }

  private def createTablesAndCopyStats(tableDetails: Seq[TableDetailsVo]): Unit = {
    tableDetails.foreach { tblDetail =>
      sparkSession.sql(tblDetail.getCreateTableStmt)
      val tblIdentifier = TableIdentifier(tblDetail.getTableName, Some("default"))
      var catalog = sparkSession.sessionState.catalog.getTableMetadata(tblIdentifier)
      val list = scala.collection.mutable.ListBuffer.empty[(String, ColumnStat)]
      catalog.schema.fields.foreach { cols =>
        val columnStat = tblDetail.getColumnStats.get(cols.name)
        if (null != columnStat) {
          list += getColumnStats(cols, columnStat)
        }
      }
      catalog = catalog.copy(stats = Some(
        CatalogStatistics(tblDetail.getTblStatsVo.getTableSize,
          Some(tblDetail.getTblStatsVo.getRowCount), list.toMap)))
      sparkSession.sessionState.catalog.alterTable(catalog)
    }
  }

  private def getColumnStats(cols: StructField, userColSats: ColStats): (String, ColumnStat) = {
    var maxValue: Option[Any] = None
    var minValue: Option[Any] = None
    cols.dataType match {
      case IntegerType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toInt)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toInt)
        }
      case LongType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toLong)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue)
        }
      case DoubleType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toDouble)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toDouble)
        }
      case FloatType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toFloat)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toFloat)
        }
      case ShortType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toShort)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toShort)
        }
      case _=>
    }
    (cols.name,
      new ColumnStat(
        userColSats.getDistinctCount,
        minValue,
        maxValue,
        userColSats.getNullCount,
        0l,
        userColSats.getMaxLength))
  }

  private def getTableProperty(tableDetails: Seq[TableDetailsVo]): String = {
    val factArray = new JsonArray
    val dimensionArray = new JsonArray
    tableDetails.foreach { tblDetail =>
      if (tblDetail.isFactTable) {
        factArray.add(new JsonPrimitive( "default."+ tblDetail.getTableName))
      } else {
        dimensionArray.add(new JsonPrimitive("default." + tblDetail.getTableName))
      }
    }
    val jsonObject = new JsonObject
    jsonObject.add("fact", factArray)
    if(dimensionArray.size < 0) {
      jsonObject.add("dimension", dimensionArray)
    }
    jsonObject.toString
  }
}

class DefaultSubExpressionManager(sparkSession: SparkSession,
    sQLConf: SQLConf,
    tableProperty: String) extends CommonSubexpressionManager(sparkSession,
  sQLConf.copy(SQLConf.CASE_SENSITIVE -> false,
    SQLConf.CBO_ENABLED -> true,
    SQLConf.buildConf("spark.mv.recommend.speedup.threshold").doubleConf.createWithDefault(0.5) ->
    0.5,
    SQLConf.buildConf("spark.mv.recommend.rowcount.threshold").doubleConf.createWithDefault(0.1) ->
    0.1,
    SQLConf.buildConf("spark.mv.recommend.frequency.threshold").doubleConf.createWithDefault(2) ->
    2,
    SQLConf.buildConf("spark.mv.tableCluster").stringConf.createWithDefault(s"""""") ->
    tableProperty)) {
}

object DefaultQueryBatchPreprocessor extends QueryBatchPreprocessor

