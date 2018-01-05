import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/1/4.
 */
public class HBaseTest {

    Configuration conf = HBaseConfiguration.create();

    public void initParams() throws IOException {
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master");
    }

    @Test
    public void testBatchInsert() throws Exception{
        initParams();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("user"));
        String filePath = "d:/workspace/pred_test";
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath),"UTF-8"));
        String line;
        List<Put> putList = new ArrayList<>();
        int batchSize = 1024;
        while((line = reader.readLine()) != null){
            String[] row = line.split(",");
            Put put = new Put(row[0].getBytes());
            String family = "model1";
            String col = "pred3";
            put.addColumn(family.getBytes(), col.getBytes(), row[1].getBytes());
            putList.add(put);
            if(putList.size() == batchSize){
                table.put(putList);
                putList.clear();
            }
        }
        table.put(putList);
    }



}
