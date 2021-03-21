package bigdata.hermesfuxi.flink.sources.parquet;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;

import java.lang.reflect.Field;

/**
 * 一个Parquet文件是由一个header以及一个或多个block块组成，以一个footer结尾。header中只包含一个4个字节的数字PAR1用来识别整个Parquet文件格式。
 * 文件中所有的metadata都存在于footer中。footer中的metadata包含了格式的版本信息，schema信息、key-value paris以及所有block中的metadata信息。
 * footer中最后两个字段为一个以4个字节长度的footer的metadata,以及同header中包含的一样的PAR1。
 */
public class ParquetUtils {
    /**
     * @description 根据schema字符串创建MessageType
     *
     * @param schema
     * @return org.apache.parquet.schema.MessageType
     */
    public static MessageType build(String schema){
        return MessageTypeParser.parseMessageType(schema);
    }

    //  ----------------  根据已有parquet文件构建  ---------------------
    // 读取一个Parquet文件时，需要完全读取Footer的meatadata，Parquet格式文件不需要读取sync markers这样的标记分割查找，因为所有block的边界都存储于footer的metadata中。
    // 所以，只需要读取目标目录中的任何一个Parquet文件即可拿到footer信息，解析footer即可得到Schema。
    /**
     * @description 根据Parquet文件创建schema
     *
     * @param parquetFilePath
     * @return org.apache.parquet.schema.MessageType
     */
    public static MessageType buildFromFile(org.apache.hadoop.fs.Path parquetFilePath) throws Exception {
        ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        MessageType schema =readFooter.getFileMetaData().getSchema();
        return schema;
    }

    /**
     * @description 根据Parquet文件创建schema
     * 勉强满足我们的需求，但每次读取parquet目录，需要先取一个文件拿来"解剖"拿到Schema才能读取全部文件，听起来就不够优雅，不用。
     * @param pathUrl
     * @return org.apache.parquet.schema.MessageType
     */
    public static MessageType buildFromFile(String pathUrl) throws Exception {
        org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path(pathUrl);
        return buildFromFile(parquetFilePath);
    }


    // ---------------------- 反射 -------------------------
    /**
     * @description 根据类信息动态解析成MessageType: 通过反射得到实体类各个字段的类型，然后对应成Parquet Schema里的类型
     * @param clazz
     * @param messageName
     * @return org.apache.parquet.schema.MessageType
     */
    public static <T> MessageType build(Class<T> clazz, String messageName){
        Field[] fields = clazz.getDeclaredFields();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for(Field field: fields){
            field.setAccessible(true);
            String[] fieldTypes = field.getType().toString().toLowerCase().split("\\.");
            String fieldType = fieldTypes[fieldTypes.length-1];
            String fieldName = field.getName();

            if("string".equals(fieldType)){
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(fieldName);
            }else if("int".equals(fieldType) || "integer".equals(fieldType) || "short".equals(fieldType)){
                builder.required(PrimitiveType.PrimitiveTypeName.INT32).named(fieldName);
            }else if("long".equals(fieldType)){
                builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
            }else if("float".equals(fieldType)){
                builder.required(PrimitiveType.PrimitiveTypeName.FLOAT).named(fieldName);
            }else if("double".equals(fieldType)){
                builder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(fieldName);
            }else if("boolean".equals(fieldType)){
                builder.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(fieldName);
            }else {
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
            }
        }
        return builder.named(messageName);
    }

    /**
     * @description 根据类信息动态解析成MessageType
     *  勉强可以应付大部分的实体类，但当实体类的字段为Timestamp、Date、List、Map等非基础类型时，则需要继续丰富上述代码，同时还需要考虑字符编码等各类问题才能使方法变得健壮
     * @param clazz
     * @return org.apache.parquet.schema.MessageType
     */
    public static <T> MessageType build(Class<T> clazz){
        String className = clazz.getName();
        return build(clazz, className);
    }

    // 用于构建ParquetPojoInputFormat中的 MessageType
    public static <T> MessageType messageTypeBuilderByAvro(Class<T> tClass){
        Schema avroSchema = ReflectData.get().getSchema(tClass);
        MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
        return messageType;
    }

    public static <T> ParquetPojoInputFormat<T> getParquetPojoInputFormat(String path, Class<T> tClass) throws Exception {
        return new ParquetPojoInputFormat<T>(
                new Path(path),
                messageTypeBuilderByAvro(tClass),
                (PojoTypeInfo<T>) PojoTypeInfo.of(tClass)
        );
    }
}
