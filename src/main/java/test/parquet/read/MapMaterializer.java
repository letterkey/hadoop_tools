package test.parquet.read;


import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

public class MapMaterializer extends RecordMaterializer<ParquetMapEntry> {
    public final MapConverter root;

    public MapMaterializer(MessageType schema) {
        this.root = new MapConverter(schema);
    }

    @Override
    public ParquetMapEntry getCurrentRecord() {
        return root.getCurrentMapEntry();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}