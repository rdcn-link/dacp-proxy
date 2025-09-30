package link.rdcn.dacp.proxy

import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.provider.DataProvider
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.{CookRequest, CookResponse, DacpServer}
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.server.{GetRequest, GetResponse}
import link.rdcn.struct.{DataFrame, DefaultDataFrame}
import link.rdcn.user.{Credentials, UserPrincipal}

/**
 * DacpServer代理类，用于内外网隔离环境下的请求转发
 */
class DacpServerProxy(
                       targetServerUrl: String,
                       dataProvider: DataProvider,
                       dataReceiver: DataReceiver,
                       authProvider: ProxyAuthorProvider
                     ) extends DacpServer(dataProvider, dataReceiver, authProvider) {

  // 内部客户端，用于连接目标 DacpServer
  private val internalClient: DacpClient = DacpClient.connect(targetServerUrl)

  override def doCook(request: CookRequest, response: CookResponse): Unit = {
    var tranformer = request.getTransformTree
    var schema = internalClient.getCookRows(tranformer.toJsonString)
    response.sendDataFrame(DefaultDataFrame(schema._1, schema._2))
  }

  override def doListDataSets(): DataFrame = {
    internalClient.get(getBaseUrl() + "/listDataSets")
  }

  override def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    internalClient.get(getBaseUrl() + listDataFrameUrl)
  }

  override def doListHostInfo(): DataFrame = {
    internalClient.get(getBaseUrl() + "/listHostInfo")
  }

  override def doGet(request: GetRequest, response: GetResponse): Unit = {
    request.getRequestURI() match {
      case "/listDataSets" =>
        try {
          response.sendDataFrame(doListDataSets())
        } catch {
          case e: Exception =>
            logger.error("Error while listDataSets", e)
            response.sendError(500, e.getMessage)
        }
      case path if path.startsWith("/listDataFrames") => {
        try {
          response.sendDataFrame(doListDataFrames(request.getRequestURI()))
        } catch {
          case e: Exception =>
            logger.error("Error while listDataFrames", e)
            response.sendError(500, e.getMessage)
        }
      }
      case "/listHostInfo" => {
        try {
          response.sendDataFrame(doListHostInfo)
        } catch {
          case e: Exception =>
            logger.error("Error while listHostInfo", e)
            response.sendError(500, e.getMessage)
        }
      }
      case otherPath =>
        val userPrincipal = request.getUserPrincipal().asInstanceOf[ProxyUserPrincipal]
        val newClient: DacpClient = DacpClient.connect(targetServerUrl, userPrincipal.credentials)
        response.sendDataFrame(newClient.get(getBaseUrl() + otherPath))
    }
  }
}

class ProxyAuthorProvider extends AuthProvider {

  override def authenticate(credentials: Credentials): UserPrincipal = ProxyUserPrincipal(credentials)

  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = true
}

case class ProxyUserPrincipal(credentials: Credentials) extends UserPrincipal {

}