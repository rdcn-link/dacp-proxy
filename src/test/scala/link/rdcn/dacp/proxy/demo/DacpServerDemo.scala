/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/16 10:29
 * @Modified By:
 */
package link.rdcn.dacp.proxy.demo

import link.rdcn.dacp.proxy.TestBase.getResourcePath
import link.rdcn.dacp.proxy.TestDemoProvider
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.DacpServer
import link.rdcn.struct.DataFrame

import java.io.File
import java.nio.file.Paths

object DacpServerDemo {
  def main(args: Array[String]): Unit = {
    val provider = new TestDemoProvider
    val fairdHome = Paths.get(getResourcePath("")).toString

    /**
     * 根据fairdHome自动读取配置文件
     * 非加密连接
     * val server = new FairdServer(provider.dataProvider, provider.authProvider, Paths.get(getResourcePath("")).toString())
     * tls加密连接
     */
    val server = DacpServer.start(new File(fairdHome), provider.dataProvider,
      new DataReceiver {
        override def receive(dataFrame: DataFrame): Unit = {}
      }, provider.authProvider)

  }
}
