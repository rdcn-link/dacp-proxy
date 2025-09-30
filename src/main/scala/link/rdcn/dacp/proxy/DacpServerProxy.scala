package link.rdcn.dacp.proxy

import link.rdcn.dacp.FairdConfig
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.provider.DataProvider
import link.rdcn.dacp.receiver.DataReceiver
import link.rdcn.dacp.server.{CookRequest, CookResponse, DacpServer}
import link.rdcn.dacp.struct.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.dacp.user.{AuthProvider, DataOperationType}
import link.rdcn.server.{ActionRequest, ActionResponse, GetRequest, GetResponse}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, StructType}
import link.rdcn.user.{Credentials, UserPrincipal}
import org.apache.jena.rdf.model.Model
import org.json.{JSONArray, JSONObject}

import java.io.StringWriter

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
    val tranformer = request.getTransformTree
    val schema = internalClient.getCookRows(tranformer.toJsonString)
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

  override def doAction(request: ActionRequest, response: ActionResponse): Unit = {
    request.getActionName() match {
      case name if name.startsWith("/getDataSetMetaData/") => {
        val prefix: String = "/getDataSetMetaData/"
        val dataSetModel: Model =
          internalClient.getDataSetMetaData(name.replaceFirst(prefix, ""))
        val writer = new StringWriter();
        dataSetModel.write(writer, "RDF/XML");
        response.send(writer.toString.getBytes("UTF-8"))

      }
      case name if name.startsWith("/getDocument/") =>
        val prefix: String = "/getDocument/"
        val dataFrameName: String = name.replaceFirst(prefix, "")
        val dataFrameDocument: DataFrameDocument
        = internalClient.getDocument(dataFrameName)
        val schema = StructType.empty.add("url", StringType).add("alias", StringType).add("title", StringType).add("dataFrameTitle", StringType)
        val stream =
          getSchema(dataFrameName).columns.map(col => col.name).map(name => Seq(dataFrameDocument.getColumnURL(name).getOrElse("")
              , dataFrameDocument.getColumnAlias(name).getOrElse(""), dataFrameDocument.getColumnTitle(name).getOrElse(""), dataFrameDocument.getDataFrameTitle().getOrElse("")))
            .map(seq => link.rdcn.struct.Row.fromSeq(seq))
        val ja = new JSONArray()
        stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
        response.send(ja.toString().getBytes("UTF-8"))
      case name if name.startsWith("/getStatistics/") =>
        val prefix: String = "/getStatistics/"
        val dataFrameName: String = name.replaceFirst(prefix, "")
        val dataFrameStatistics: DataFrameStatistics =
          internalClient.getStatistics(dataFrameName)
        val jo = new JSONObject()
        jo.put("byteSize", dataFrameStatistics.byteSize)
        jo.put("rowCount", dataFrameStatistics.rowCount)
        response.send(jo.toString().getBytes("UTF-8"))
      case name if name.startsWith("getDataFrameSize") =>
        val prefix: String = "/getDataFrameSize/"
        val dataFrameSize: Long =
          dataProvider.getDataStreamSource(name.replaceFirst(prefix, "")).rowCount
        response.send(dataFrameSize.toString.getBytes("UTF-8"))
      case otherPath => response.sendError(400, s"Action $otherPath Invalid")
    }
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