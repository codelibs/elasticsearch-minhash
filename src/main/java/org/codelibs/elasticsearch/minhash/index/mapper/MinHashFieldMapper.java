package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper.ValueAndBoost;

import com.carrotsearch.hppc.ObjectArrayList;

public class MinHashFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "minhash";
    private static final ParseField COMPRESS = new ParseField("compress").withAllDeprecated("no replacement, implemented at the codec level");
    private static final ParseField COMPRESS_THRESHOLD = new ParseField("compress_threshold").withAllDeprecated("no replacement");
    private NamedAnalyzer minhashAnalyzer;
    private String copyBitsTo;
    private Method parseCopyMethod;

    private static MinHashFieldMapper.Builder minhashField(final String name) {
        return new MinHashFieldMapper.Builder(name);
    }

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new MinHashFieldType();

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, MinHashFieldMapper> {

        private NamedAnalyzer minhashAnalyzer;

        private String copyBitsTo;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public MinHashFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            ((MinHashFieldType)fieldType).setTryUncompressing(context.indexCreatedVersion().before(Version.V_2_0_0_beta1));
            return new MinHashFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo,
                    minhashAnalyzer, copyBitsTo);
        }

        public void minhashAnalyzer(final NamedAnalyzer minhashAnalyzer) {
            this.minhashAnalyzer = minhashAnalyzer;
        }

        public void copyBitsTo(final String copyBitsTo) {
            this.copyBitsTo = copyBitsTo;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            MinHashFieldMapper.Builder builder = minhashField(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String fieldName = entry.getKey();
                final Object fieldNode = entry.getValue();
                if (parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1) &&
                        (parserContext.parseFieldMatcher().match(fieldName, COMPRESS) || parserContext.parseFieldMatcher().match(fieldName, COMPRESS_THRESHOLD))) {
                    iterator.remove();
                } else if (fieldName.equals("minhash_analyzer")
                        && fieldNode != null) {
                    final NamedAnalyzer analyzer = parserContext
                            .analysisService().analyzer(fieldNode.toString());
                    builder.minhashAnalyzer(analyzer);
                    iterator.remove();
                } else if (fieldName.equals("copy_bits_to")
                        && fieldNode != null) {
                    builder.copyBitsTo(fieldNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class MinHashFieldType extends MappedFieldType {
        private boolean tryUncompressing = false;

        public MinHashFieldType() {}

        protected MinHashFieldType(MinHashFieldType ref) {
            super(ref);
            this.tryUncompressing = ref.tryUncompressing;
        }

        @Override
        public MappedFieldType clone() {
            return new MinHashFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            MinHashFieldType that = (MinHashFieldType) o;
            return Objects.equals(tryUncompressing, that.tryUncompressing);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), tryUncompressing);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            MinHashFieldType other = (MinHashFieldType)fieldType;
            if (tryUncompressing() != other.tryUncompressing()) {
                conflicts.add("mapper [" + names().fullName() + "] has different [try_uncompressing] (IMPOSSIBLE)");
            }
        }

        public boolean tryUncompressing() {
            return tryUncompressing;
        }

        public void setTryUncompressing(boolean tryUncompressing) {
            checkIfFrozen();
            this.tryUncompressing = tryUncompressing;
        }

        @Override
        public BytesReference value(Object value) {
            if (value == null) {
                return null;
            }

            BytesReference bytes;
            if (value instanceof BytesRef) {
                bytes = new BytesArray((BytesRef) value);
            } else if (value instanceof BytesReference) {
                bytes = (BytesReference) value;
            } else if (value instanceof byte[]) {
                bytes = new BytesArray((byte[]) value);
            } else {
                try {
                    bytes = new BytesArray(Base64.decode(value.toString()));
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to convert bytes", e);
                }
            }
            try {
                if (tryUncompressing) { // backcompat behavior
                    return CompressorFactory.uncompressIfNeeded(bytes);
                } else {
                    return bytes;
                }
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to decompress source", e);
            }
        }

        @Override
        public Object valueForSearch(Object value) {
            return value(value);
        }
    }

    protected MinHashFieldMapper(String simpleName, MappedFieldType fieldType,
            MappedFieldType defaultFieldType, Settings indexSettings,
            MultiFields multiFields, CopyTo copyTo,
            NamedAnalyzer minhashAnalyzer, String copyBitsTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.minhashAnalyzer = minhashAnalyzer;
        this.copyBitsTo = copyBitsTo;

        try {
            Class<?> docParserClazz = FieldMapper.class.getClassLoader()
                    .loadClass("org.elasticsearch.index.mapper.DocumentParser");
            parseCopyMethod = docParserClazz.getDeclaredMethod("parseCopy",
                    new Class[] { String.class, ParseContext.class });
            parseCopyMethod.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to access DocumentParser#parseCopy(String, ParseContext).",
                    e);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        ValueAndBoost valueAndBoost = parseCreateFieldForString(context, fieldType().nullValueAsString(), fieldType().boost());
        String text = valueAndBoost.value();
        if (text == null) {
            return;
        }

        byte[] value = MinHash.calculate(minhashAnalyzer, text);
        if (value == null) {
            return;
        }

        if (copyBitsTo != null) {
            final CopyBitsTo.Builder builder = new CopyBitsTo.Builder(parseCopyMethod);
            builder.add(copyBitsTo);
            final CopyBitsTo copyBitsTo = builder.build();
            copyBitsTo.parse(context.createExternalValueContext(MinHash
                    .toBinaryString(value)));
        }

        if (fieldType().stored()) {
            fields.add(new Field(fieldType().names().indexName(), value, fieldType()));
        }

        if (fieldType().hasDocValues()) {
            CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context.doc().getByKey(fieldType().names().indexName());
            if (field == null) {
                field = new CustomBinaryDocValuesField(fieldType().names().indexName(), value);
                context.doc().addWithKey(fieldType().names().indexName(), field);
            } else {
                field.add(value);
            }
        }

    }

    /**
     * Parse a field as though it were a string.
     * @param context parse context used during parsing
     * @param nullValue value to use for null
     * @param defaultBoost default boost value returned unless overwritten in the field
     * @return the parsed field and the boost either parsed or defaulted
     * @throws IOException if thrown while parsing
     */
    public static ValueAndBoost parseCreateFieldForString(ParseContext context, String nullValue, float defaultBoost) throws IOException {
        if (context.externalValueSet()) {
            return new ValueAndBoost((String) context.externalValue(), defaultBoost);
        }
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return new ValueAndBoost(nullValue, defaultBoost);
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            XContentParser.Token token;
            String currentFieldName = null;
            String value = nullValue;
            float boost = defaultBoost;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                        value = parser.textOrNull();
                    } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else {
                        throw new IllegalArgumentException("unknown property [" + currentFieldName + "]");
                    }
                }
            }
            return new ValueAndBoost(value, boost);
        }
        return new ValueAndBoost(parser.textOrNull(), defaultBoost);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }


    @Override
    protected void doXContentBody(XContentBuilder builder,
            boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("minhash_analyzer", minhashAnalyzer.name());
        if (copyBitsTo != null) {
            builder.field("copy_bits_to", copyBitsTo);
        }
    }

    public static class CustomBinaryDocValuesField extends NumberFieldMapper.CustomNumericDocValuesField {

        private final ObjectArrayList<byte[]> bytesList;

        private int totalSize = 0;

        public CustomBinaryDocValuesField(String name, byte[] bytes) {
            super(name);
            bytesList = new ObjectArrayList<>();
            add(bytes);
        }

        public void add(byte[] bytes) {
            bytesList.add(bytes);
            totalSize += bytes.length;
        }

        @Override
        public BytesRef binaryValue() {
            try {
                CollectionUtils.sortAndDedup(bytesList);
                int size = bytesList.size();
                final byte[] bytes = new byte[totalSize + (size + 1) * 5];
                ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
                out.writeVInt(size);  // write total number of values
                for (int i = 0; i < size; i ++) {
                    final byte[] value = bytesList.get(i);
                    int valueLength = value.length;
                    out.writeVInt(valueLength);
                    out.writeBytes(value, 0, valueLength);
                }
                return new BytesRef(bytes, 0, out.getPosition());
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }

    public static class CopyBitsTo {

        private final List<String> copyBitsToFields;
        private Method parseCopyMethod;

        private CopyBitsTo(final List<String> copyToFields, Method parseCopyMethod) {
            copyBitsToFields = copyToFields;
            this.parseCopyMethod = parseCopyMethod;
        }

        /**
         * Creates instances of the fields that the current field should be copied to
         */
        public void parse(final ParseContext context) throws IOException {
            for (final String field : copyBitsToFields) {
                try {
                    parseCopyMethod.invoke(null,
                            new Object[] { field, context });
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Failed to invoke parseCopy method.", e);
                }
            }
        }

        public XContentBuilder toXContent(final XContentBuilder builder,
                final Params params) throws IOException {
            if (!copyBitsToFields.isEmpty()) {
                builder.startArray("copy_bits_to");
                for (final String field : copyBitsToFields) {
                    builder.value(field);
                }
                builder.endArray();
            }
            return builder;
        }

        public static class Builder {
            private final List<String> copyToBuilders = new ArrayList<>();
            private Method parseCopyMethod;

            public Builder(Method parseCopyMethod){
                this.parseCopyMethod = parseCopyMethod;
            }

            public Builder add(final String field) {
                copyToBuilders.add(field);
                return this;
            }

            public CopyBitsTo build() {
                return new CopyBitsTo(Collections.unmodifiableList(copyToBuilders), parseCopyMethod);
            }
        }

        public List<String> copyToFields() {
            return copyBitsToFields;
        }
    }

}
