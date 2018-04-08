package database.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableOutputFormat}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by DELL_PC on 2018/3/21.
 */
object HbaseOperation {
  def onePut() {
    val conf = new SparkConf().setMaster("local[2]").setAppName("hbaseop")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 to 5)
    rdd.foreachPartition(x => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//      hbaseConf.set("hbase.default.for.version.skip", "true")
      val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
      val table = hbaseConn.getTable(TableName.valueOf("word"))
      x.foreach(value => {
        var put = new Put(Bytes.toBytes(value.toString))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(s"vaule ${value}"))
        table.put(put)
      })
    })
    sc.stop()
  }
  def batchPut(): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("hbaseop")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 to 5)
    rdd.map(value => {
      val put = new Put(Bytes.toBytes(s"cf ${value}"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(s"value ${value}"))
      put
    }).foreachPartition(iter => {
      val hbaseConf = HBaseConfiguration.create()
      val jobConf = new JobConf(hbaseConf)
      jobConf.set("habse.zookeeper.quorum", "127.0.0.1")
      jobConf.set("habse.zookeeper.property.clientPort", "2181")
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      val table = new HTable(jobConf, TableName.valueOf("word"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(iter.toSeq))
    })
    sc.stop()
  }

  def mapreducePut(): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("hbaseop")
    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()
    val jobConf = new JobConf(hbaseConf)
    jobConf.set("hbase.zookeeper.quorum", "127.0.0.1")
    jobConf.set("hbase.zookeeper.property.clientPort", "2181")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "word")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val rdd = sc.makeRDD(1 to 5)
    rdd.map(value =>{
      val put = new Put(Bytes.toBytes(s"rowkey ${value}"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(s"value $value"))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
    sc.stop()
  }

  def putByRDD(sc: SparkContext, hbaseConf: Configuration, tabelname: String): Unit ={
    val jobConf = new JobConf(hbaseConf)
//    jobConf.set("hbase.zookeeper.quorum", "127.0.0.1")
//    jobConf.set("hbase.zookeeper.property.clientPort", "2181")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tabelname)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val rdd = sc.makeRDD(1 to 5)
    rdd.map(value => {
      val put = new Put(Bytes.toBytes(s"rowkey $value"))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes(s"value 2 $value"))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }

  def createHbaseTable(tablename: String, hbaseAdmin:Admin): Unit ={
    val table = TableName.valueOf(tablename)

    if(hbaseAdmin.isTableAvailable(table)){
      println(s"Table $tablename has been created !!!")
    } else {
      println(s"Table $tablename is not available !!!")
      val tableDes = new HTableDescriptor(table)
      tableDes.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")))
      hbaseAdmin.createTable(tableDes)
      println(s"Table $tablename has been created successfully !!!")
    }
  }

  def createHbaseAdmin(hbaseConf: Configuration): Admin ={
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val hbaseAdmin = hbaseConn.getAdmin
    hbaseAdmin
  }

  def createHbaseConf(): Configuration = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf
  }

  def queryHbase(hBaseConf: Configuration, tablename: String, sc: SparkContext): Unit ={
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
    val rdd = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
    classOf[Result])
    rdd.foreach({ case (_, result) => {
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("cf1".getBytes, "c1".getBytes))
      println(s"Row Key: $key value: $value ")
    }})
  }

  def scanHbase(sc: SparkContext, hBaseConf: Configuration, startRowKey: String, endRowKey: String, tablename: String): Unit ={
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
//    hBaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf1")
    hBaseConf.set(TableInputFormat.SCAN_COLUMNS, "cf1")
    hBaseConf.set(TableInputFormat.SCAN_ROW_START, startRowKey)
    hBaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRowKey)
    val rdd = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
    classOf[Result])
    rdd.foreach({ case (_, result) => {
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("cf1".getBytes, "c2".getBytes))
      println(s"Row Key: $key value: $value ")
    }})
  }

  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("hbaseop")
    val sc = new SparkContext(conf)
    val hbaseConf = createHbaseConf()
    val hBaseAdmin = createHbaseAdmin(hbaseConf)
    val tablename = "CreateTest"
//    createHbaseTable("CreateTest",hBaseAdmin)
//    putByRDD(sc, hBaseAdmin.getConfiguration, tablename)
//    queryHbase(hbaseConf, tablename, sc)
    scanHbase(sc, hbaseConf, "rowkey 2", "rowkey 4", tablename)
    sc.stop()
  }
}
