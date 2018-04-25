package database.mysql;

import breeze.optimize.linear.LinearProgram;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DELL_PC on 2018/4/24.
 */
public class MySQLOperation {
    private static String driver = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://localhost:3306";
    private String db = "testdb";
    private String encoding = "utf-8";
    private String character = "?userUnicode=true&characterEncoding=";
    private String user = "root";
    private String pwd = "";


    MySQLOperation(String url, String db, String encoding, String user, String pwd) {
        this.url = url;
        this.db = db;
        this.encoding = encoding;
        this.user = user;
        this.pwd = pwd;
        this.character = this.character + encoding;
    }

    MySQLOperation(String db, String user, String pwd) {
        this.db = db;
        this.user = user;
        this.pwd = pwd;
        this.character = this.character + encoding;
    }

    MySQLOperation() {
        this.character = this.character + encoding;
    }

    private Connection conn = null;
    private Statement stmt= null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;
    private void createConn() {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url + "/" + db + character, user, pwd);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to MySQL! Connected Info: [database = " + db + "; user = " + user + "]");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connDB() {
        if (conn == null) {
            try {
                createConn();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (stmt == null) {
            try {
                stmt = conn.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public List query(String sql) {
        connDB();
        int count;
        List<Map> list = new ArrayList<>();
        try {
            rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd;
            rsmd = rs.getMetaData();
            count = rsmd.getColumnCount();
            while(rs.next()) {
                Map map = new HashMap<>();
                for (int i = 1; i <= count; i ++) {
                    String field = rsmd.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    map.put(field.toLowerCase(), value);
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public void executeStmt(String sql) {
        connDB();
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insert(String table, String[] cols, List<String[]> values) {
        String colStr = "";
        if (cols != null)
            colStr = "(" + StringUtils.join(cols, ",") + ")";
        StringBuilder sb = new StringBuilder();
        for (String[] row: values) {
            sb.append("(" + StringUtils.join(row, ",") + "),");
        }
        sb.deleteCharAt(sb.length() - 1);
        String sql = "insert into " + table + colStr + " values " + sb;
        System.out.println(sql);
        executeStmt(sql);
    }

    public void insertByPs() {
        connDB();
        try {
            String sql = "insert into info values (?,?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, "0005");
            ps.setString(2, "Alice");
            ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateByPs() {
        connDB();
        try {
            ps = conn.prepareStatement("update info set name = ? where id = ?");
            ps.setString(2, "0002");
            ps.setString(1, "Bob");
            ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteByPs() {
        connDB();
        try {
            ps = conn.prepareStatement("delete from info where name = ?");
            ps.setString(1, "Anna");
            ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void show(List<Map> rs) {
        for (Map map: rs) {
            System.out.println(map);
        }
    }

    public void closeDB() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void readBySpark() {
        SparkSession spark = SparkSession.builder()
                .appName("ReadBySpark")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("bdtable", db + ".info")
                .option("user", user)
                .option("password", pwd)
                .load();
        jdbcDF.show();
        spark.stop();
    }
}

class MySQLTest {
    public static void main(String[] args) {
        MySQLOperation op = new MySQLOperation("testdb", "root", "");
//        op.deleteByPs();
//        List rs = op.query("select * from info");
//        op.show(rs);
//        String[] cols = {"name", "id"};
//        List<String[]> values = new ArrayList<>();
//        values.add(new String[]{"'Sam'", "'0003'"});
//        values.add(new String[]{"'Anna'", "'0004'"});
//        op.insertDB("info", cols, values);
        op.readBySpark();
        op.closeDB();
    }
}
