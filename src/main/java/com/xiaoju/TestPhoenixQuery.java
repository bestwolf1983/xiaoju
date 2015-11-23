package com.xiaoju;

import java.sql.*;

/**
 * Created by cenyuhai on 2015/11/11.
 */
public class TestPhoenixQuery {
  public static void main(String[] args) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:phoenix:bigdata-arch-hdp277.bh:2181");
    String sql  = "";
    Statement statement = conn.createStatement();
    ResultSet result = statement.executeQuery(sql);
    ResultSetMetaData meta = result.getMetaData();
    int cols = meta.getColumnCount();

    while(result.next()) {
      System.out.println("result: " + result.getLong(1));
    }

    statement.close();
    conn.close();
  }
}
