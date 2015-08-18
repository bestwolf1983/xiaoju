package com.xiaoju;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class TestCrateInsert {
    private static final String dbClassName = "io.crate.client.jdbc.CrateDriver";
    private static final String CONNECTION = "crate://localhost:4300";
    private static final String USER = "root";
    private static final String PASSWORD = "";
    private static final String[] cities = new String[]{"beijing", "shenzhen", "shanghai", "guangzhou", "chengdu"};
    private static String schema = "order_id,create_time,order_type,pid,city_id,timeout,p_phone_num,p_create_time,p_setup_time,p_ip,p_imei,p_suuid,p_alipay_zhenshen,p_alipay_account,p_weixin_openid,p_weixin_zhenshen,p_order_channel,p_datatype,p_networktype,p_pay_total,p_pay_fee,p_pay_time,p_pay_type,p_pay_ip,p_pay_datatype,p_pay_networktype,p_pay_alipay_zhenshen,p_pay_alipay_account,p_pay_weixin_openid,p_pay_weixin_zhenshen,p_coupon_id ,p_coupon_trackid ,p_coupon_amount ,is_timeout,did ,d_phone_num ,d_route_id ,d_alipay_zhenshen ,d_alipay_account ,d_weixin_zhenshen ,d_weixin_openid , d_grab_time,d_order_channel ,d_datatype ,d_networktype,d_imei ,d_ip ,d_suuid ,d_arrived_ip ,d_arrived_datatype ,d_arrived_networktype ,d_arrived_time ,order_status ,cancel_time ,contact_type,promoter,contact_time ,complain_type ,complain_time ,remark ,var1 ,var2,create_date,update_time,last_edit_time";


    private static void executeSQL(Connection conn, String sql) throws SQLException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(sql);
        }
    }

    private static void insert() {
        /*Properties properties = new Properties();
        properties.put("user", USER);
        properties.put("password", PASSWORD);*/
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        try {
            String fileName = "/data/xiaoju/output.txt";
            File file = new File(fileName);
            BufferedReader reader = null;
            String[] data = null;
            Date createDate = null;
            String[] fields = schema.split(",");

            Connection conn = DriverManager.getConnection(CONNECTION);
            // conn.setAutoCommit(false);
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            StringBuilder insertSql = null;
            String date = "";
            PreparedStatement ps = null;

            insertSql = new StringBuilder("insert into sf_order(" + schema + ")values(");
            for(int i=0;i< fields.length;i++) {
                insertSql.append("?,");
            }

            if (insertSql.lastIndexOf(",") > -1) {
                insertSql = insertSql.deleteCharAt(insertSql.length() - 1);
            }

            insertSql.append(")");
            ps = conn.prepareStatement(insertSql.toString());
            long start = System.currentTimeMillis();
            long end = 0L;
            while ((tempString = reader.readLine()) != null) {
                line++;
                data = tempString.split("\t");
/*              insertSql = new StringBuilder("insert into sf_order(" + schema + ")values(");             */
                createDate = timeFormat.parse(data[data.length - 1]);
                date = dateFormat.format(createDate);
                ps.setLong(1, Long.parseLong(data[0]));
                ps.setInt(2, Integer.parseInt(date));
                for (int i = 2; i < data.length; i++) {
                    ps.setString(i+1, data[i].trim().replaceAll("'", ""));
                }

                ps.addBatch();

/*              insertSql.append(data[1] + "," + date + ",");
                for (int i = 2; i < data.length; i++) {
                    insertSql.append("'" + data[i].trim().replaceAll("'", "") + "',");
                }
                if (insertSql.lastIndexOf(",") > -1) {
                    insertSql = insertSql.deleteCharAt(insertSql.length() - 1);
                }

                insertSql.append(")");
                executeSQL(conn, insertSql.toString());*/
                // System.out.println(insertSql.toString());

                if (line % 10000 == 0) {
                    ps.executeBatch();
                    conn.commit();
                    end = System.currentTimeMillis();
                    System.out.println("insert 1w records use " + (end - start)/1000 + " s");
                    start = end;
                }

                if (line % 50000 == 0) {
                    System.out.println("handle " + line + " records!");
                }
            }
            ps.executeBatch();
            // conn.commit();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void update() {
        try {
            Thread.sleep(10000);
        } catch (Exception ex) {
        }

        Properties properties = new Properties();
        properties.put("user", USER);
        properties.put("password", PASSWORD);
        try {
            Connection conn = DriverManager.getConnection(CONNECTION, properties);
            Random random = new Random();
            String udpateDdl = "";
            long count = 0;
            long start = System.currentTimeMillis();
            long end = 0L;
            while (!Thread.interrupted()) {
                udpateDdl = "update tbl set test2='1' where id=" + random.nextInt(10000);
                executeSQL(conn, udpateDdl);
                count++;
                if (count % 10000 == 0) {
                    end = System.currentTimeMillis();
                    System.out.println("Update 10000 records use " + (end - start) / 1000 + "s");
                    start = end;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        Class.forName(dbClassName);
        // ResetEnvironment();
        long start = System.currentTimeMillis();
        insert();
        long end = System.currentTimeMillis();
        System.out.println("Use " + (end - start) / 1000 + "s");
    }

}

