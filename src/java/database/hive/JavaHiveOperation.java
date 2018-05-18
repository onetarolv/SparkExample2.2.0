package database.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by DELL_PC on 2018/5/5.
 */
public class JavaHiveOperation {
    private final static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private final static String url = "jdbc:hive2://localhost:10000/default";

    Connection conn = null;
    Statement stmt = null;
    public void createConn() {
        if (conn != null)
            return;
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void createStatement() {
        if (stmt == null) {
            try {
                stmt = conn.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void query(String sql) {
        createConn();
        createStatement();
        try {
            stmt.executeQuery(sql);
            System.out.println("execute \"" + sql + "\" successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class JavaHiveOperationTestMain{
    public static void main(String[] args) {
        JavaHiveOperation jhop = new JavaHiveOperation();
        jhop.query("create database userdb");
        jhop.close();
    }
}
