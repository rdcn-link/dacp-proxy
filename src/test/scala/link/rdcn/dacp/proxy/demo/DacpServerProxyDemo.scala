/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/14 16:31
 * @Modified By:
 */
package link.rdcn.dacp.proxy.demo

import link.rdcn.dacp.proxy.ConfigLoader.{fairdConfig, proxyConfig}
import link.rdcn.dacp.proxy.{DacpServerProxy, TestDemoProvider}
import link.rdcn.dacp.proxy.TestBase._
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.DacpServer
import link.rdcn.struct.DataFrame

import java.io.File
import java.nio.file.Paths


object ServerProxyDemo extends TestDemoProvider {
  def main(args: Array[String]): Unit = {
    val provider = new TestDemoProvider
    val fairdHome = Paths.get(getResourcePath("")).toString
    val protocolScheme = "dacp"
//    val baseUrl = s"$protocolScheme://${provider.fairdConfig.hostPosition}:${provider.fairdConfig.hostPort}"
//
//    DacpServerProxy.start(baseUrl, proxyConfig)
  }
}

