/**
 * @Author Yomi
 * @Description:
 * @Data 2025/10/14 16:31
 * @Modified By:
 */
package link.rdcn.dacp.proxy.demo

import link.rdcn.dacp.proxy.TestBase._
import link.rdcn.dacp.client.DacpClient
import link.rdcn.dacp.proxy.{DacpClientProxy, TestBase, TestDemoProvider}
import link.rdcn.dacp.recipe.{ExecutionResult, Flow, FlowNode}
import link.rdcn.dacp.struct.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.dacp.{ConfigKeys, ResourceKeys}
import link.rdcn.struct._
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils
import org.apache.jena.rdf.model.Model

import java.io.{File, FileOutputStream}
import java.nio.file.{Path, Paths}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
object ClientDemo {
  val provider = new TestDemoProvider
  val tlsFile = new File(Paths.get(getResourcePath(""), "faird").toString)

  def main(args: Array[String]): Unit = {
    // 通过用户名密码非加密连接FairdClient
        val dcp: DacpClientProxy = DacpClientProxy.connect("dacp://10.0.82.210:3102", UsernamePassword("15117913512@126.com", "admin#U*Q!."));
    // 通过用户名密码tls加密连接FairdClient
    //  val dcp: DacpClientProxy = DacpClient.connectTLS("dacp://localhost:3102", tlsFile, UsernamePassword("admin@instdb.cn", "admin001"))
    // 匿名连接FairdClient
    //    val dcAnonymous: DacpClientProxy = DacpClient.connect("dacp://localhost:3102", Credentials.ANONYMOUS());

    dcp.listDataSetNames().foreach { dsName =>
      println(dsName)
      dcp.listDataFrameNames(dsName).foreach{dfName =>
        println(dfName)
        dcp.get(s"dacp://10.0.90.43:3102/$dfName").foreach(println)}
    }


    //获得host基本信息
    println("--------------打印host基本信息--------------")
    val hostInfo: Map[String, String] = dcp.getHostInfo
    println(hostInfo(ConfigKeys.FAIRD_HOST_NAME))
    println(hostInfo(ConfigKeys.FAIRD_HOST_TITLE))
    println(hostInfo(ConfigKeys.FAIRD_HOST_PORT))
    println(hostInfo(ConfigKeys.FAIRD_HOST_POSITION))
    println(hostInfo(ConfigKeys.FAIRD_HOST_DOMAIN))
    println(hostInfo(ConfigKeys.FAIRD_TLS_ENABLED))
    println(hostInfo(ConfigKeys.FAIRD_TLS_CERT_PATH))
    println(hostInfo(ConfigKeys.FAIRD_TLS_KEY_PATH))


    //获得服务器资源信息
    println("--------------打印服务器资源信息--------------")
    val serverResourceInfo: Map[String, String] = dcp.getServerResourceInfo
    println(serverResourceInfo(ResourceKeys.CPU_CORES))
    println(serverResourceInfo(ResourceKeys.CPU_USAGE_PERCENT))
    println(serverResourceInfo(ResourceKeys.JVM_MAX_MEMORY_MB))
    println(serverResourceInfo(ResourceKeys.JVM_USED_MEMORY_MB))
    println(serverResourceInfo(ResourceKeys.JVM_FREE_MEMORY_MB))
    println(serverResourceInfo(ResourceKeys.JVM_TOTAL_MEMORY_MB))
    println(serverResourceInfo(ResourceKeys.SYSTEM_MEMORY_TOTAL_MB))
    println(serverResourceInfo(ResourceKeys.SYSTEM_MEMORY_FREE_MB))
    println(serverResourceInfo(ResourceKeys.SYSTEM_MEMORY_USED_MB))


    //打开非结构化数据的文件列表数据帧
    val dfBin: DataFrame = dcp.getByPath("/bin")

    /**
     * 获得数据帧的Document，包含由Provider定义的SchemaURI等信息
     * 用户可以控制没有信息时输出的字段
     */
    println("--------------打印数据帧Document--------------")
    val dataFrameDocument: DataFrameDocument = dcp.getDocument("/bin")
    val schemaURL: String = dataFrameDocument.getSchemaURL().getOrElse("schemaURL not found")
    val columnURL: String = dataFrameDocument.getColumnURL("file_name").getOrElse("columnURL not found")
    val columnAlias: String = dataFrameDocument.getColumnAlias("file_name").getOrElse("columnAlias not found")
    val columnTitle: String = dataFrameDocument.getColumnTitle("file_name").getOrElse("columnTitle not found")
    println(schemaURL)
    println(columnURL)
    println(columnAlias)
    println(columnTitle)
    println(dfBin.schema)

    //获得数据帧大小
    println("--------------打印数据帧行数和大小--------------")
    val dataFrameStatistics: DataFrameStatistics = dcp.getStatistics("/bin")
    val dataFrameRowCount: Long = dataFrameStatistics.rowCount
    val dataFrameSize: Long = dataFrameStatistics.byteSize
    println(dataFrameRowCount)
    println(dataFrameSize)

    //可以对数据帧进行操作 比如foreach 每行数据为一个Row对象，可以通过Tuple风格访问每一列的值
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

    /**
     * 获取数据
     * 对数据进行collect操作可以将数据帧的所有行收集到内存中，但是要注意内存溢出的问题
     * limit操作可以限制返回的数据行数，防止内存溢出
     * 还可以打开CSV文件数据帧
     */
    val dfCsv: DataFrame = dcp.getByPath("/csv/data_1.csv")
    val csvRows: Seq[Row] = dfCsv.limit(1).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 数据帧--------------")
    csvRows.foreach(println)

    //编写map算子的匿名函数对数据帧进行操作
    val mappedRows: Seq[Row] = dfCsv.map(x => Row(x._1)).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过map操作后的数据帧--------------")
    mappedRows.take(3).foreach(println)

    //编写filter算子的匿名函数对数据帧进行操作
    val filteredRows: Seq[Row] = dfCsv.filter({ row =>
      val id: Long = row._1.asInstanceOf[Long]
      id <= 1L
    }).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过filter操作后的数据帧--------------")
    filteredRows.foreach(println)

    //select可以通过列名得到指定列的数据
    val selectedRows: Seq[Row] = dfCsv.select("id").collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过select操作后的数据帧--------------")
    selectedRows.take(3).foreach(println)

    /**
     * 自定义算子和DAG执行图对数据帧进行操作
     * 构建数据源节点
     */
    val sourceNodeA: FlowNode = FlowNode.source("/csv/data_1.csv")
    val sourceNodeB: FlowNode = FlowNode.source("/csv/data_2.csv")

    /**
     * 也可以构建自定义算子节点对象
     * 自定义一个map算子 比如对第一列加1
     */
    val udfMap: FlowNode = FlowNode.ofScalaFunction(dataFrame =>
      dataFrame.map(row => Row.fromTuple(row.getAs[Long](0) + 1, row.get(1))))

    //自定义一个filter算子 比如只保留小于等于3的行
    val udfFilter: FlowNode = FlowNode.ofScalaFunction(dataFrame =>
      dataFrame.filter((row: Row) => {
        val value: Long = row._1.asInstanceOf[Long]
        value <= 3L
      }))

    /**
     * 对于线性依赖可以通过pipe直接构造DAG
     * 至少一个节点
     */
    val transformerDAGMin: Flow = Flow.pipe(sourceNodeA)
    val minDAGDfs: ExecutionResult = dcp.execute(transformerDAGMin)
    println("--------------打印最小DAG直接获取的数据帧--------------")
    //当结果只有一个数据帧获得唯一结果
    minDAGDfs.single().limit(3).foreach(row => println(row))
    //通过名字获得指定数据帧结果
    minDAGDfs.get("1").limit(3).foreach(row => println(row))
    //获得处理结果的Map，name是数据帧名称，df是对应的数据帧
    minDAGDfs.map().foreach { case (name, df) => df.limit(3).foreach(row => println(name, row)) }

    //可以多个节点
    val transformerDAGPipe: Flow = Flow.pipe(sourceNodeA, udfFilter, udfMap)
    val pipeDAGDfs: ExecutionResult = dcp.execute(transformerDAGPipe)
    println("--------------打印执行链式DAG的数据帧--------------")
    pipeDAGDfs.map().foreach { case (_, df) => df.foreach(row => println(row)) }

    /**
     * 也可以通过构建边Map和节点Map构建DAG执行图
     * 构建DAG执行图A -> B ，A是数据源节点B是自定义filter算子
     */
    val nodesMap: Map[String, FlowNode] = Map(
      "A" -> sourceNodeA,
      "B" -> udfFilter
    )
    //构建边Map，一个节点可以有多个下游节点
    val edgesMap: Map[String, Seq[String]] = Map(
      "A" -> Seq("B")
    )
    //通过边和节点Map构建DAG执行图
    val transformerDAG: Flow = Flow(nodesMap, edgesMap)
    //执行DAG图，返回一个数据帧列表
    val simpleDfs: ExecutionResult = dcp.execute(transformerDAG)
    println("--------------打印自定义filter算子操作后的数据帧--------------")
    simpleDfs.map().foreach { case (_, df) => df.foreach(row => println(row)) }

    /**
     * 可以构建更复杂的多数据源节点和操作的DAG
     * A  B
     * |/\|
     * C  D
     */
    val transformerComplexDAG: Flow = Flow(
      Map("A" -> sourceNodeA,
        "B" -> sourceNodeB,
        "C" -> udfFilter,
        "D" -> udfMap),
      Map("A" -> Seq("C", "D"),
        "B" -> Seq("C", "D"))
    )
    val complexDfs: ExecutionResult = dcp.execute(transformerComplexDAG)
    println("--------------打印执行自定义DAG后的数据帧--------------")
    complexDfs.map().foreach { case (_, df) => df.limit(3).foreach(row => println(row)) }

    //可以通过put操作向服务器上传DataFrame
    val dataStreamSource: DataStreamSource = provider.dataProvider.getDataStreamSource("/csv/data_1.csv")
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    val batchSize = 100
    val msg = dcp.put(dataFrame, batchSize)
    println(msg)
  }
}