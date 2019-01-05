package test.parquet.read;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetReader;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

public class Main {
    public static void main(String[] args) throws IOException {
        Path path = new Path("/test/test_dict.parquet");
        ParquetReader<ParquetMapEntry> reader = new ParquetReader<ParquetMapEntry>(path, new ReadSupport<ParquetMapEntry>() {
            @Override
            public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
                System.out.println(fileSchema);
                return new ReadContext(fileSchema);
            }

            @Override
            public RecordMaterializer<ParquetMapEntry> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
                return new MapMaterializer(fileSchema);
            }
        });

        ParquetMapEntry r = reader.read();
        while (r != null) {
            System.out.println(r);
            r = reader.read();
        }
    }
}