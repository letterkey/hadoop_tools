package test.parquet.read;


import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

public class MapConverter extends GroupConverter {
    private final Converter converters[];
    private final MapConverter parentConverter;
    private ParquetMapEntry mapEntry;

    public MapConverter(GroupType schema) {
        this(schema, null);
    }

    public MapConverter(GroupType schema, MapConverter parent) {
        this.converters = new Converter[schema.getFieldCount()];
        this.parentConverter = parent;

        int i = 0;
        for (Type field : schema.getFields()) {
            converters[i++] = createConverter(field);
        }
    }

    private Converter createConverter(Type field) {
        return (field.isPrimitive() ?
                new SimplePrimitiveConverter(field.getName()) :
                new MapConverter(field.asGroupType(),  this));
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    ParquetMapEntry getCurrentMapEntry() {
        return mapEntry;
    }

    @Override
    public void start() {
        mapEntry = new ParquetMapEntry();
    }

    @Override
    public void end() {
        if (parentConverter != null) {
            parentConverter.getCurrentMapEntry().addChildEntry(mapEntry);
        }
    }

    private class SimplePrimitiveConverter extends PrimitiveConverter {
        protected final String fieldName;

        public SimplePrimitiveConverter(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public void addBinary(Binary value) {
            mapEntry.set(fieldName, value.toByteBuffer());
        }

        @Override
        public void addBoolean(boolean value) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void addDouble(double value) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void addFloat(float value) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void addInt(int value) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void addLong(long value) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}