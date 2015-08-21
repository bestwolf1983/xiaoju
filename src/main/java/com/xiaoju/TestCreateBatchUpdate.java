package com.xiaoju;

import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.client.CrateClient;
import io.crate.shade.org.elasticsearch.common.settings.ImmutableSettings;
import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by cenyuhai on 2015/7/30.
 */
public class TestCreateBatchUpdate {
    public static void main(String[] args) throws Exception {

        CrateClient client = new CrateClient("localhost:4300");
        System.out.println("start!");
        String updateSql = "update sf_order2 set test1 = ? where create_time=? and order_id = ?";

        String fileName = "/home/xiaoju/output.txt";
        //String fileName = "D:/1.txt";
        File file = new File(fileName);
        BufferedReader reader = null;
        String[] data = null;
        Random random = new Random();
        String tempString = null;

        int count = Integer.valueOf(args[1]);

        if (args[0].equals("1")) {

            SQLBulkRequest bulkRequest = new SQLBulkRequest(
                    updateSql,
                    new Object[][]{new Object[]{"bar", 4}, new Object[]{"baz", 6}});
            SQLBulkResponse bulkResponse = client.bulkSql(bulkRequest).actionGet();
            for (SQLBulkResponse.Result result : bulkResponse.results()) {
                System.out.println(result.rowCount());
            }
        } else if(args[0].equals("2")) {
            reader = new BufferedReader(new FileReader(file));
            Object[][] bulkArgs = new Object[count][3];
            int line = 0;
            while ((tempString = reader.readLine()) != null) {
                line++;
                data = tempString.split("\t");
            /*Object[] temp = new Object[2];
            temp[0] = String.valueOf(random.nextInt(10000));
            temp[1] = Integer.valueOf(data[1].trim());*/
                // temp[0] = String.valueOf(random.nextInt(10000));
                // temp[1] = Long.valueOf(data[1].trim());

                // bulkArgs[(line - 1) % 10] = new Object[]{String.valueOf(random.nextInt(10000)), Integer.valueOf(data[1].trim())};
                bulkArgs[(line - 1) % count] = new Object[]{String.valueOf(random.nextInt(10000)), 21696014};
                if (line % count == 0) {
                    SQLBulkRequest bulkRequest = new SQLBulkRequest(updateSql, bulkArgs);
                    System.out.println("start to bulk");
                    SQLBulkResponse bulkResponse = client.bulkSql(bulkRequest).actionGet();
                    for (SQLBulkResponse.Result result : bulkResponse.results()) {
                        System.out.println(result.rowCount());
                    }
                    bulkArgs = new Object[count][2];
                }
            }

        } else if(args[0].equals("3")){
            reader = new BufferedReader(new FileReader(file));
            Object[][] bulkArgs = new Object[count][3];
            int line = 0;
            long start = System.currentTimeMillis();
            long end = 0L;
            SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            Date createDate = null;
            String date = "";
            while ((tempString = reader.readLine()) != null) {
                line++;
                data = tempString.split("\t");
                createDate = timeFormat.parse(data[data.length - 1]);
                date = dateFormat.format(createDate);
            /*Object[] temp = new Object[2];
            temp[0] = String.valueOf(random.nextInt(10000));
            temp[1] = Integer.valueOf(data[1].trim());*/
                // temp[0] = String.valueOf(random.nextInt(10000));
                // temp[1] = Long.valueOf(data[1].trim());

                bulkArgs[(line - 1) % count] = new Object[]{ data[1].trim(), Integer.parseInt(date), Long.valueOf(data[0].trim())};
                //bulkArgs[(line - 1) % 10] = new Object[]{String.valueOf(random.nextInt(10000)), 21696014};
                if (line % count == 0) {
                    SQLBulkRequest bulkRequest = new SQLBulkRequest(updateSql, bulkArgs);
                    System.out.println("start to bulk");
                    SQLBulkResponse bulkResponse = client.bulkSql(bulkRequest).actionGet();
/*                    for (SQLBulkResponse.Result result : bulkResponse.results()) {
                        System.out.println(result.rowCount());
                    }*/
                    bulkArgs = new Object[count][3];
                }

                if(line % 10000 ==0) {
                    end = System.currentTimeMillis();
                    System.out.println("update 1w records use " + (end - start) + " ms");
                    start = end;
                }


            }

        }


    }
}
