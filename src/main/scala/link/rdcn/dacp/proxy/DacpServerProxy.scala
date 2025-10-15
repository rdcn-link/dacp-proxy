package link.rdcn.dacp.proxy

import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.provider.DataProvider
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.{CookRequest, CookResponse, DacpServer}
import link.rdcn.dacp.struct.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.server.{ActionRequest, ActionResponse, GetRequest, GetResponse}
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, StructType}
import link.rdcn.user.{Credentials, UserPrincipal}
import org.apache.jena.rdf.model.Model

import java.util.concurrent.ConcurrentHashMap

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
  private val clientCache = new ConcurrentHashMap[Credentials, DacpClient]()
  private def getInternalClient(credentials: Credentials): DacpClient = {
    if(clientCache.contains(credentials)) clientCache.get(credentials)
    else {
      val client = DacpClient.connect(targetServerUrl, credentials)
      clientCache.put(credentials, client)
      client
    }
  }

  override def doCook(request: CookRequest, response: CookResponse): Unit = {
    val internalClient = getInternalClient(request.getRequestUserPrincipal()
      .asInstanceOf[ProxyUserPrincipal].credentials)
    val tranformer = request.getTransformTree
    val schema = internalClient.getCookRows(tranformer.toJsonString)
    response.sendDataFrame(DefaultDataFrame(schema._1, schema._2))
  }

  def doListDataSets(internalClient: DacpClient): DataFrame = {
    internalClient.get(getBaseUrl() + "/listDataSets")
  }

  def doListDataFrames(listDataFrameUrl: String, internalClient: DacpClient): DataFrame = {
    internalClient.get(getBaseUrl() + listDataFrameUrl)
  }

  def doListHostInfo(internalClient: DacpClient): DataFrame = {
    internalClient.get(getBaseUrl() + "/listHostInfo")
  }

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = {
    val internalClient = getInternalClient(request.getUserPrincipal()
      .asInstanceOf[ProxyUserPrincipal].credentials)
    request.getActionName() match {
      case name if name.startsWith("/getDataSetMetaData/") ||
        name.startsWith("/getDataFrameMetaData/") ||
        name.startsWith("/getDocument/") ||
        name.startsWith("/getStatistics/") ||
        name.startsWith("/getDataFrameSize/") =>
        val resultBytes: Array[Byte] = internalClient.doAction(name)
        response.send(resultBytes)
      case name if name == "/getTargetServerUrl" =>
        response.send(targetServerUrl.getBytes("UTF-8"))
      case otherPath => response.sendError(400, s"Action $otherPath Invalid")
    }
  }


  override def doGet(request: GetRequest, response: GetResponse): Unit = {
    val internalClient = getInternalClient(request.getUserPrincipal()
      .asInstanceOf[ProxyUserPrincipal].credentials)
    request.getRequestURI() match {
      case "/listDataSets" =>
        try {
          response.sendDataFrame(doListDataSets(internalClient))
        } catch {
          case e: Exception =>
            logger.error("Error while listDataSets", e)
            response.sendError(500, e.getMessage)
        }
      case path if path.startsWith("/listDataFrames") => {
        try {
          response.sendDataFrame(doListDataFrames(request.getRequestURI(), internalClient))
        } catch {
          case e: Exception =>
            logger.error("Error while listDataFrames", e)
            response.sendError(500, e.getMessage)
        }
      }
      case "/listHostInfo" => {
        try {
          response.sendDataFrame(doListHostInfo(internalClient))
        } catch {
          case e: Exception =>
            logger.error("Error while listHostInfo", e)
            response.sendError(500, e.getMessage)
        }
      }
      case otherPath =>
        response.sendDataFrame(internalClient.get(getBaseUrl() + otherPath))
    }
  }
}

object DacpServerProxy{
  def start(targetServerUrl: String, fairdConfig: FairdConfig): DacpServerProxy = {
    val dacpServerProxy: DacpServerProxy = new DacpServerProxy(targetServerUrl, dataProvider, dataReceiver, new ProxyAuthorProvider)
    dacpServerProxy.start(fairdConfig)
    dacpServerProxy
  }

  private val dataProvider = new DataProvider {
    override def listDataSetNames(): java.util.List[String] = ???

    override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = ???

    override def listDataFrameNames(dataSetId: String): java.util.List[String] = ???

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = ???

    override def getDocument(dataFrameName: String): DataFrameDocument = ???

    override def getStatistics(dataFrameName: String): DataFrameStatistics = ???

    override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = ???
  }
  private val dataReceiver = new DataReceiver {
    override def receive(dataFrame: DataFrame): Unit = ???
  }
}

class ProxyAuthorProvider extends AuthProvider {

  override def authenticate(credentials: Credentials): UserPrincipal = ProxyUserPrincipal(credentials)

  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean = true
}

case class ProxyUserPrincipal(credentials: Credentials) extends UserPrincipal