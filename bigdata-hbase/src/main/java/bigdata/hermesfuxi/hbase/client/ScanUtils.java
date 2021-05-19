package bigdata.hermesfuxi.hbase.client;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

public class ScanUtils {
    public static void main(String[] args) throws Exception {
        Table tb_user = HBaseClientUtils.getTable("tb_user");

        ResultScanner scanner = tb_user.getScanner(Bytes.toBytes("cf1"));
        Iterator<Result> iterator = scanner.iterator();
        Result result = iterator.next();
        if(result.advance()){
            byte[] rowBytes = result.getRow();
//            List<Cell> columnCells = result.getColumnCells();
//            byte[] valueBytes = result.getValue();

        }
    }
}
