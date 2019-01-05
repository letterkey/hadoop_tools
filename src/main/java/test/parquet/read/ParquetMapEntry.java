package test.parquet.read;


import com.google.common.base.Throwables;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class ParquetMapEntry {
    private static final CharsetDecoder UTF8_DECODER = Charset.forName("UTF-8").newDecoder();

    private String key;
    private ByteBuffer value;

    public void set(String key, ByteBuffer value) {

        try {
            //variable key contains the field name and the value contains the value for that field
            if (key.equals("key")) {
                this.key = UTF8_DECODER.decode(value).toString();
            }

            if (key.equals("value")) {
                this.value = value;
            }

        } catch (CharacterCodingException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Have to propagate the key/value pairs all the way up to the parent entries
     */
    public void addChildEntry(ParquetMapEntry r) {
        this.key = r.key;
        this.value = r.value;
    }

    @Override
    public String toString() {
        try {
            return String.format("(%s -> %s)", this.key, UTF8_DECODER.decode(this.value));
        } catch (CharacterCodingException e) {
            throw Throwables.propagate(e);
        }
    }
}