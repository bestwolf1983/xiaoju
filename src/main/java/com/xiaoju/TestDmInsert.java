package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.sql.*;
import java.util.HashMap;


public class TestDmInsert {
    private static final String dbClassName = "io.crate.client.jdbc.CrateDriver";
    private static final String CONNECTION = "crate://localhost:4300";

    private static void executeSQL(Connection conn, String sql) throws SQLException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(sql);
        }
    }

    private static void insert(String split) {
        try {
            Connection conn = DriverManager.getConnection(CONNECTION);

            String tempString = null;
            int line = 1;
            StringBuilder insertSql = null;
            PreparedStatement ps = null;
            String[] fields = TableSchema.fieldSchema().split("\n");
            HashMap<Integer,String> map = new HashMap<Integer,String>();
            String temp = null;
            for(int i=0;i<fields.length;i++) {
                temp = fields[i].split("\t")[1];
                map.put(i, temp.trim());
            }

            int fieldLength = fields.length;

            insertSql = new StringBuilder(TableSchema.insertBiTagSql());
            insertSql.append("(");

            for (int i = 0; i < fieldLength; i++) {
                insertSql.append("?,");
            }

            if (insertSql.lastIndexOf(",") > -1) {
                insertSql = insertSql.deleteCharAt(insertSql.length() - 1);
            }

            insertSql.append(")");

            String[] data = null;
            ps = conn.prepareStatement(insertSql.toString());
            long start = System.currentTimeMillis();
            long end = 0L;

            BufferedReader reader = null ;
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] files = fs.listStatus(new Path("/user/rd/bi_dm/tmp_dm_tag_pass_dd_kd_merge_1/year=2015/month=07/day=06/pidsn=-1"));

            int startFileIndex = 0;
            if (split.equals("1")) {
                startFileIndex = 0;
            } else if (split.equals("2")) {
                startFileIndex = 40;
            } else if (split.equals("3")) {
                startFileIndex = 80;
            }

            int endFileIndex = startFileIndex + 40;
            if (endFileIndex > files.length) {
                endFileIndex = files.length;
            }

            for (int i = startFileIndex; i < endFileIndex; i++) {
                InputStream in = null;
                try {
                    in = fs.open(files[i].getPath());
                    reader = new BufferedReader(new InputStreamReader(in));
                    while ((tempString = reader.readLine()) != null) {
                        line++;
                        data = tempString.split("\t");
                        for (int j = 0; j < data.length; j++) {
                            temp = map.get(j).trim();
                            if(temp.startsWith("int")) {
                                ps.setInt(j + 1, Integer.parseInt(data[i].trim().replaceAll("'", "")));
                            } else if(temp.startsWith("double")) {
                                ps.setDouble(j + 1, Double.parseDouble(data[i].trim().replaceAll("'", "")));
                            } else {
                                ps.setString(j + 1, data[i].trim().replaceAll("'", ""));
                            }
                        }
                        ps.addBatch();

                        if (line % 10000 == 0) {
                            ps.executeBatch();
                            conn.commit();
                            end = System.currentTimeMillis();
                            System.out.println("insert 1w records use " + (end - start) / 1000 + " s");
                            start = end;
                        }

                        if (line % 50000 == 0) {
                            System.out.println("handle " + line + " records!");
                        }
                    }
                    ps.executeBatch();
                    conn.commit();
                    in.close();
                    reader.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        Class.forName(dbClassName);
        long start = System.currentTimeMillis();
        insert(args[0]);
        long end = System.currentTimeMillis();
        System.out.println("Use " + (end - start) / 1000 + "s");
    }

}

