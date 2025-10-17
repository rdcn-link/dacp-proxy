package link.rdcn.dacp.proxy

import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.proxy.TestBase.testClassesDir
import link.rdcn.dacp.server.DacpServer
import link.rdcn.struct.Blob
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils
import org.junit.jupiter.api.Test

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/14 16:30
 * @Modified By:
 */
class DacpServerProxyTest {
  @Test
  def run(): Unit = {
    val config = new FairdConfig();
    config.setHostPort(3333);
    config.setHostPosition("localhost");
    val serverProxy = DacpServerProxy.start("dacp://10.0.90.43:3102", config);

    val client = DacpClientProxy.connect("dacp://localhost:3333", new UsernamePassword("15117913512@126.com", "admin#U*Q!."))
    client.listDataSetNames().foreach { dsName =>
      println(dsName)
      client.listDataFrameNames(dsName).foreach{dfName =>
        println(dfName)
        client.get(s"dacp://10.0.90.43:3102/$dfName").foreach(println)}
    }

    //        val serverProxy = DacpServer.start("dacp://10.0.90.43:3102", config);

    //    val client = DacpClient.connect("dacp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"))
    //    val df = client.getByPath("/bin")
    //    val blob = client.getBlobById("ef10a903-d905-4486-801e-c5dfa5967e33")
    //    blob.asInstanceOf[Blob].offerStream((in)=>{println(in)})
    //    val path: Path = Paths.get(testClassesDir.getParentFile.getParentFile.getAbsolutePath, "src", "test", "resources", "data", "output", "1.bin")
    //    blob.asInstanceOf[Blob].offerStream(inputStream => {
    //      val path: Path = Paths.get("C:\\Users\\ASUS\\Documents\\Projects\\PycharmProjects\\dacp-proxy\\test_output\\bin\\1.bin")
    //      val outputStream = new FileOutputStream(path.toFile)
    //      IOUtils.copy(inputStream, outputStream)
    //      outputStream.close()
    //    })
    //    df.foreach(row=>row._7.asInstanceOf[Blob].offerStream((in)=>{println(in)}))
    //    println(client.listDataSetNames())
    //        client.listDataSetNames().foreach(println)

  }

  @Test
  def runDacpClientServer(): Unit = {
    val client = DacpClientProxy.connect("dacp://0.0.0.0:3102", UsernamePassword("admin@instdb.cn", "admin001"))
    //    val blob = client.getBlobById("ef10a903-d905-4486-801e-c5dfa5967e33")
    val df = client.getByPath("/bin")
    df.foreach { row =>
      val name: String = row._1.asInstanceOf[String]
      val byteSize: Long = row.getAs[Long](3)
      val blob = row._7.asInstanceOf[Blob]
      blob.offerStream(inputStream => {
        val path: Path = Paths.get("C:\\Users\\ASUS\\Documents\\Projects\\PycharmProjects\\dacp-proxy\\test_output\\bin\\2.bin")
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)
        outputStream.close()
      })
      println(row)
      println(name)
      println(byteSize)
    }

  }

}
