package link.rdcn.dacp.proxy

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/14 16:30
 * @Modified By:
 */
import link.rdcn.dacp.ConfigKeys.{FAIRD_HOST_DOMAIN, FAIRD_HOST_NAME, FAIRD_HOST_PORT, FAIRD_HOST_POSITION, FAIRD_HOST_TITLE, FAIRD_TLS_CERT_PATH, FAIRD_TLS_ENABLED, FAIRD_TLS_KEY_PATH, LOGGING_FILE_NAME, LOGGING_LEVEL_ROOT, LOGGING_PATTERN_CONSOLE, LOGGING_PATTERN_FILE}
import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.ResourceKeys.{CPU_CORES, CPU_USAGE_PERCENT, JVM_FREE_MEMORY_MB, JVM_MAX_MEMORY_MB, JVM_TOTAL_MEMORY_MB, JVM_USED_MEMORY_MB, SYSTEM_MEMORY_FREE_MB, SYSTEM_MEMORY_TOTAL_MB, SYSTEM_MEMORY_USED_MB}
import link.rdcn.dacp.proxy.ConfigLoader.{fairdConfig, proxyConfig}
import link.rdcn.dacp.proxy.TestBase.{genModel, testClassesDir}
import link.rdcn.dacp.proxy.TestProvider.{dataProvider, expectedHostInfo, getExpectedDataFrameSize}
import link.rdcn.dacp.proxy.demo.ClientDemo.provider
import link.rdcn.struct.{Blob, DataFrame, DataStreamSource, DefaultDataFrame, Row}
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class DacpClientProxyTest extends TestProvider{
  val protocolScheme = "dacp"
  val baseUrl = s"$protocolScheme://${fairdConfig.hostPosition}:${fairdConfig.hostPort}"
  val proxyUrl = s"$protocolScheme://${proxyConfig.hostPosition}:${proxyConfig.hostPort}"
  val dsp = DacpServerProxy.start(baseUrl, proxyConfig)
  val dcp = DacpClientProxy.connect(proxyUrl, UsernamePassword("admin@instdb.cn", "admin001"))
  val csvModel: Model = genModel
  val binModel: Model = genModel


  @Test
  def testConnect_InvalidUrl_ShouldThrowException(): Unit = {
    val invalidUrl = "http://invalid-schema.com"
    val exception =  assertThrows(classOf[IllegalArgumentException],
      () => DacpClientProxy.connect(invalidUrl)
    )

    val expectedMessage = s"Invalid DACP URL:"
    assertEquals(expectedMessage, exception.getMessage, "Invalid DACP URL exception should be thrown")
  }

  @Test
  def testConnectTLS_InvalidUrl_ShouldThrowException(): Unit = {
    val malformedUrl = "dacp:/malformed-url"
    val exception = assertThrows(classOf[IllegalArgumentException],
      () =>{DacpClientProxy.connectTLS(malformedUrl)}
    )

    val expectedMessage = "Invalid DACP URL: The URL is not in valid format, should be dacp://<host>[:port]"
    assertEquals(expectedMessage, exception.getMessage, "异常信息应指明URL格式错误。")
  }

  @Test
  def getTargetServerUrl_shouldCallDoActionAndReturnDecodedString(): Unit = {
    val dcp = DacpClientProxy.connect(proxyUrl)
    assertEquals(baseUrl, dcp.getTargetServerUrl, "target Url should be matched")
  }

  @Test
  def putTest(): Unit = {
    val dataStreamSource: DataStreamSource = provider.dataProvider.getDataStreamSource("/csv/data_1.csv")
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    val batchSize = 100
    dcp.put(dataFrame, batchSize)
  }

  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(dataProvider.listDataSetNames().toSet, dcp.listDataSetNames().toSet, "ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(dataProvider.listDataFrameNames("csv").toSet, dcp.listDataFrameNames("csv").toSet, "ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(dataProvider.listDataFrameNames("bin").toSet, dcp.listDataFrameNames("bin").toSet, "ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertTrue(csvModel.isIsomorphicWith(dcp.getDataSetMetaData("csv")), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertTrue(binModel.isIsomorphicWith(dcp.getDataSetMetaData("bin")), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }

  @Test
  def testSchema(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertTrue(csvModel.isIsomorphicWith(dcp.getDataSetMetaData("csv")), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertTrue(binModel.isIsomorphicWith(dcp.getDataSetMetaData("bin")), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }

  @Test
  def testGetHostInfo(): Unit = {
    val allKeys: Set[String] = Set(
      FAIRD_HOST_DOMAIN,
      FAIRD_HOST_TITLE,
      FAIRD_HOST_NAME,
      FAIRD_HOST_PORT,
      FAIRD_HOST_POSITION,
      LOGGING_FILE_NAME,
      LOGGING_LEVEL_ROOT,
      LOGGING_PATTERN_FILE,
      LOGGING_PATTERN_CONSOLE,
      FAIRD_TLS_ENABLED,
      FAIRD_TLS_CERT_PATH,
      FAIRD_TLS_KEY_PATH
    )
    val hostInfo = dcp.getHostInfo
    allKeys.foreach(key => {
      assertTrue(hostInfo.contains(key), s"实际结果中缺少键：$key")
      assertEquals(expectedHostInfo(key), hostInfo(key), s"键 '$key' 的值与预期不符！")
    }
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("/csv/data_1.csv"))
  def testGetDataFrameSize(dataFrameName: String): Unit = {
    assertEquals(getExpectedDataFrameSize(dataFrameName), dcp.getDataFrameSize(dataFrameName), "GetDataFrameSize接口输出与预期不符！")
  }

  @Test
  def testGetServerResourceInfo(): Unit = {
    val expectedResourceInfo = dcp.getServerResourceInfo

    val allKeys: Set[String] = Set(
      CPU_CORES,
      CPU_USAGE_PERCENT,
      JVM_TOTAL_MEMORY_MB,
      JVM_FREE_MEMORY_MB,
      JVM_USED_MEMORY_MB,
      JVM_MAX_MEMORY_MB,
      SYSTEM_MEMORY_USED_MB,
      SYSTEM_MEMORY_FREE_MB,
      SYSTEM_MEMORY_TOTAL_MB
    )
    val serverResouceInfo = dcp.getServerResourceInfo
    allKeys.foreach(key => {
      assertTrue(serverResouceInfo.contains(key), s"实际结果中缺少键：$key")
    }
    )
  }

  @Test
  def getBlobTest(): Unit = {
    val dfBin = dcp.get(s"$baseUrl/bin")
    println("--------------打印非结构化数据文件列表数据帧--------------")
    dfBin.foreach((row: Row) => {
      //通过Tuple风格访问
      val name: String = row._1.asInstanceOf[String]
      //通过下标访问
      val blob: Blob = row.get(6).asInstanceOf[Blob]
      //通过getAs方法获取列值，该方法返回Option类型，如果找不到对应的列则返回None
      val byteSize: Long = row.getAs[Long](3)
      //除此之外列值支持的类型还包括：Integer, Long, Float, Double, Boolean, byte[]
      //offerStream用于接受一个用户编写的处理blob InputStream的函数并确保其关闭
      val path: Path = Paths.get(testClassesDir.getParentFile.getParentFile.getAbsolutePath, "src", "test", "resources", "data", "output", name)
      blob.offerStream(inputStream => {
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)
        outputStream.close()
      })
    //或者直接获取blob的内容，得到byte数组
      println(row)
      println(name)
      println(byteSize)
    })
  }



}

object DacpClientProxyTest {

}