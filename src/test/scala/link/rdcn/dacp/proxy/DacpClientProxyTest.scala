package link.rdcn.dacp.proxy

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/14 16:30
 * @Modified By:
 */
import link.rdcn.dacp.proxy.ConfigLoader.{fairdConfig, proxyConfig}
import link.rdcn.dacp.proxy.TestBase.testClassesDir
import link.rdcn.struct.{Blob, Row}
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}

class DacpClientProxyTest extends TestProvider{
  val protocolScheme = "dacp"
  val baseUrl = s"$protocolScheme://${fairdConfig.hostPosition}:${fairdConfig.hostPort}"
  val proxyUrl = s"$protocolScheme://${proxyConfig.hostPosition}:${proxyConfig.hostPort}"
  val dsp = DacpServerProxy.start(baseUrl, proxyConfig)

  /**
   * 测试 `DacpClientProxy.connect` 方法。
   * 当提供一个无效的URL（例如，错误的协议头）时，应抛出 IllegalArgumentException。
   */
  @Test
  def testConnect_InvalidUrl_ShouldThrowException(): Unit = {
    val invalidUrl = "http://invalid-schema.com"
    val exception =  assertThrows(classOf[IllegalArgumentException],
      () => DacpClientProxy.connect(invalidUrl)
    )

    val expectedMessage = s"Invalid DACP URL:"
    assertEquals(expectedMessage, exception.getMessage, "Invalid DACP URL exception should be thrown")
  }

  /**
   * 测试 `DacpClientProxy.connectTLS` 方法。
   * 当提供一个格式不正确的DACP URL时，应抛出 IllegalArgumentException。
   */
  @Test
  def testConnectTLS_InvalidUrl_ShouldThrowException(): Unit = {
    val malformedUrl = "dacp:/malformed-url"
    val exception = assertThrows(classOf[IllegalArgumentException],
      () =>{DacpClientProxy.connectTLS(malformedUrl)}
    )

    val expectedMessage = "Invalid DACP URL: The URL is not in valid format, should be dacp://<host>[:port]"
    assertEquals(expectedMessage, exception.getMessage, "异常信息应指明URL格式错误。")
  }

  /**
   * 测试 `getTargetServerUrl` 实例方法。
   * 该测试验证此方法是否正确调用了父类的 `doAction` 方法，并正确地将返回的字节数组解码为UTF-8字符串。
   *
   * 注意：由于 `DacpClientProxy` 的构造函数是私有的（private），我们必须使用Java反射来创建实例以进行测试。
   * 这通常表明原始代码的可测试性有待提高。
   */
  @Test
  def getTargetServerUrl_shouldCallDoActionAndReturnDecodedString(): Unit = {
    val dcp = DacpClientProxy.connect(proxyUrl)
    assertEquals(baseUrl, dcp.getTargetServerUrl, "target Url should be matched")
    dcp.close()
  }

  /**
   * 这是一个占位测试，用于说明 `get` 方法的测试难点。
   */
  @Test
  def testGetTest(): Unit = {
    val dcp = DacpClientProxy.connect(proxyUrl, UsernamePassword("admin@instdb.cn", "admin001"))
//    println(dcp.listDataSetNames())
//    println(dcp.listDataFrameNames("csv"))
//    println(dcp.getServerResourceInfo)
//    println(dcp.getHostInfo)
//    println(dcp.getDocument("/bin"))
//    println(dcp.getDataSetMetaData("csv"))
//    println(dcp.getDataFrameSize("/csv/data_1.csv"))
//    println(dcp.getStatistics("/csv/data_1.csv"))
//    println(dcp.getByPath(s"/csv/data_1.csv"))
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