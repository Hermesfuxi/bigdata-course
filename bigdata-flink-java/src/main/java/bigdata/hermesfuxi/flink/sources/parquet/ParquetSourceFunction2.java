package bigdata.hermesfuxi.flink.sources.parquet;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

public class ParquetSourceFunction2 implements SourceFunction<String> {
    private boolean flag = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (flag) {
            Path path = new Path("hdfs://hadoop-master:9000/datayi/dicts/geodict/part-00000-ac206f6b-fd35-4c87-bf80-8897a825db44-c000.snappy.parquet");
            Configuration conf = new Configuration();
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            FileMetaData metadata = readFooter.getFileMetaData();
            MessageType schema = metadata.getSchema();

            ParquetFileReader parquetFileReader = new ParquetFileReader(conf, metadata, path, readFooter.getBlocks(), schema.getColumns());

            PageReadStore pages = null;
            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                MessageColumnIO columnio = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnio.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < pages.getRowCount() - 1; i++) {
                    Group group = recordReader.read();
                    String geohash = group.getString("province", 0);
                    ctx.collect(geohash);
                }
            }

            // do whatever logic suits you to stop "watching" the file
            Thread.sleep(60000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
