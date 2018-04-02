package org.apache.carbondata.mv.tool

import java.io.File

import scala.collection.JavaConverters._

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.mv.tool.offlineadvision.OfflineAdvisior
import org.apache.carbondata.mv.tool.reader.JsonFileReader


object MvOfflineAdvisiorTool {

  def main(args: Array[String]): Unit = {
    val querySqls = JsonFileReader.readQuerySql("D:/mvinput/querylog.sql").asScala
    val tableDetails = JsonFileReader.readTableDetails("D:/mvinput/tableDetails.txt").asScala
    val outLocation: String = "D:/mvout/mv-candidate.sql"
    val spark = createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("INFO")
    val rootPath = new File("D:/mvadvisior").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/log4j.properties")
    OfflineAdvisior(spark).adviseMV(querySqls, tableDetails, outLocation)

  }
  def currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath

  def createCarbonSession(appName: String, workThreadNum: Int = 1): SparkSession = {
    val sparkConf = new SparkConf(loadDefaults = true)
    val builder = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Carbon Thrift Server(uses CarbonSession)")
      .enableHiveSupport()

    val storePath = if (args.length > 0) args.head else null

    val spark = builder.getOrCreateCarbonSession(storePath)
    val warmUpTime = CarbonProperties.getInstance().getProperty("carbon.spark.warmUpTime", "5000")
    try {
      Thread.sleep(Integer.parseInt(warmUpTime))
    } catch {
      case e: Exception =>
        val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
        LOG.error(s"Wrong value for carbon.spark.warmUpTime $warmUpTime " +
                  "Using default Value and proceeding")
        Thread.sleep(5000)
    }
    spark
  }
}
