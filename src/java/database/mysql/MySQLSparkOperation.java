package database.mysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import utils.bean.Person;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by DELL_PC on 2018/4/25.
 */
public class MySQLSparkOperation {
    private static String driver = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://localhost:3306";
    private String db = "testdb";
    private String encoding = "utf-8";
    private String character = "?userUnicode=true&characterEncoding=";
    private String user = "root";
    private String pwd = "";
    private SparkSession spark = SparkSession.builder()
            .appName("Spark")
            .master("local[2]")
            .getOrCreate();
    public void readTableBySparkOption(String tablename) {
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", db + "." + tablename)
                .option("user", user)
                .option("password", pwd)
                .load();
        jdbcDF.show();
        spark.stop();
    }

    public void readTableBySparkProperty(String tablename) {
        Properties connectedProperties = new Properties();
        connectedProperties.put("user", user);
        connectedProperties.put("password", pwd);
        connectedProperties.put("customSchema", "id STRING, name STRING");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc(url, "(select id from testdb.info) t", connectedProperties);
//                .jdbc(url, db + "." + tablename, connectedProperties);
        jdbcDF2.show();
    }

    public void readTableBySparkOptions(String tablename) {
        Map<String, String> map = new HashMap<String, String>(){{
            put("driver", driver);
            put("url", url);
            put("dbtable", db + "." + tablename);
            put("user", user);
            put("password", pwd);
        }};
        Dataset<Row> jdbcDF = spark.read().format("jdbc").options(map).load();
        jdbcDF.show();
    }


    public SparkSession createSparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("ReadBySpark")
                .master("local[2]")
                .getOrCreate();
        return spark;
    }


    public static class Info implements Serializable {
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        Info(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public Dataset<Row> createDF() {
        JavaRDD<Info> infoRDD = spark.read()
                .textFile("data/syntax/sql/info")
                .javaRDD()
                .map(line -> {
                    String[] values = line.split(",");
                    Info info = new Info(values[0], values[1]);
                    return info;
                });
        Dataset<Row> infoDF = spark.createDataFrame(infoRDD, Info.class);
        infoDF.printSchema();
        return infoDF;
    }

    public void saveDataBySparkOption(String tablename) {
        Dataset<Row> df = createDF();
        df.write().mode(SaveMode.Overwrite).format("jdbc")
                .option("url", url)
                .option("dbtable", db + "." + tablename)
                .option("user", user)
                .option("password", pwd)
                .save();
    }

    public void saveDataBySparkProperty(String tablename) {
        Dataset<Row> df = createDF();
        Properties connectedProperties = new Properties();
        connectedProperties.put("user", user);
        connectedProperties.put("password", pwd);
        df.write()
                .option("createTableColumnTypes", "id char(20), name char(30)")
                .jdbc(url, db + "." + tablename, connectedProperties);
    }

    public void stop() {
        spark.stop();
    }
}

class MySQLSparkOpTest{
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        MySQLSparkOperation test = new MySQLSparkOperation();
//        test.saveDataBySparkOption("StuInfo");
        test.readTableBySparkProperty("StuInfo");
    }
}
