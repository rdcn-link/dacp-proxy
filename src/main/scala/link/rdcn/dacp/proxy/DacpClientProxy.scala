/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/9 17:55
 * @Modified By:
 */
package link.rdcn.dacp.proxy

import link.rdcn.client.{RemoteDataFrameProxy, UrlValidator}
import link.rdcn.dacp.client.DacpClient
import link.rdcn.operation.SourceOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials

class DacpClientProxy private(host: String, port: Int, useTLS: Boolean = false) extends DacpClient(host, port, useTLS) {
  override def get(url: String): DataFrame = {
    val urlValidator = new UrlValidator(prefixSchema)
    if (urlValidator.isPath(url)) RemoteDataFrameProxy(SourceOp(url), super.getRows) else {
      urlValidator.validate(url) match {
        case Right(value) => RemoteDataFrameProxy(SourceOp(url), getRows)
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }
  }

  def getTargetServerUrl: String = {
    new String(doAction("/getTargetServerUrl"), "UTF-8")
  }

}


object DacpClientProxy {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClientProxy = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClientProxy(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }

  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClientProxy = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClientProxy(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
