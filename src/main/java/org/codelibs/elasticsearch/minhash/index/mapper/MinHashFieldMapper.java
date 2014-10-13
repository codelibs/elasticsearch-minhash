package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.hppc.ObjectArrayList;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;

public class MinHashFieldMapper extends AbstractFieldMapper<BytesReference> {

    public static final String CONTENT_TYPE = "minhash";

    public static MinHashFieldMapper.Builder minhashField(final String name) {
        return new MinHashFieldMapper.Builder(name);
    }

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final long COMPRESS_THRESHOLD = -1;

        public static final FieldType FIELD_TYPE = new FieldType(
                AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setIndexed(false);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends
            AbstractFieldMapper.Builder<Builder, MinHashFieldMapper> {

        private Boolean compress = null;

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        private NamedAnalyzer minhashAnalyzer;

        private String copyBitsTo;

        public Builder(final String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder compress(final boolean compress) {
            this.compress = compress;
            return this;
        }

        public Builder compressThreshold(final long compressThreshold) {
            this.compressThreshold = compressThreshold;
            return this;
        }

        @Override
        public MinHashFieldMapper build(final BuilderContext context) {
            return new MinHashFieldMapper(buildNames(context), fieldType,
                    docValues, compress, compressThreshold, postingsProvider,
                    docValuesProvider, fieldDataSettings,
                    multiFieldsBuilder.build(this, context), copyTo,
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
        public Mapper.Builder parse(final String name,
                final Map<String, Object> node,
                final ParserContext parserContext)
                throws MapperParsingException {
            final MinHashFieldMapper.Builder builder = minhashField(name);
            parseField(builder, name, node, parserContext);
            for (final Map.Entry<String, Object> entry : node.entrySet()) {
                final String fieldName = Strings.toUnderscoreCase(entry
                        .getKey());
                final Object fieldNode = entry.getValue();
                if (fieldName.equals("compress") && fieldNode != null) {
                    builder.compress(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("compress_threshold")
                        && fieldNode != null) {
                    if (fieldNode instanceof Number) {
                        builder.compressThreshold(((Number) fieldNode)
                                .longValue());
                        builder.compress(true);
                    } else {
                        builder.compressThreshold(ByteSizeValue
                                .parseBytesSizeValue(fieldNode.toString())
                                .bytes());
                        builder.compress(true);
                    }
                } else if (fieldName.equals("minhash_analyzer")
                        && fieldNode != null) {
                    final NamedAnalyzer analyzer = parserContext
                            .analysisService().analyzer(fieldNode.toString());
                    builder.minhashAnalyzer(analyzer);
                } else if (fieldName.equals("copy_bits_to")
                        && fieldNode != null) {
                    builder.copyBitsTo(fieldNode.toString());
                }
            }
            return builder;
        }
    }

    private Boolean compress;

    private long compressThreshold;

    private NamedAnalyzer minhashAnalyzer;

    private String copyBitsTo;

    protected MinHashFieldMapper(final Names names, final FieldType fieldType,
            final Boolean docValues, final Boolean compress,
            final long compressThreshold,
            final PostingsFormatProvider postingsProvider,
            final DocValuesFormatProvider docValuesProvider,
            @Nullable final Settings fieldDataSettings,
            final MultiFields multiFields, final CopyTo copyTo,
            final NamedAnalyzer minhashAnalyzer, final String copyBitsTo) {
        super(names, 1.0f, fieldType, docValues, null, null, postingsProvider,
                docValuesProvider, null, null, fieldDataSettings, null,
                multiFields, copyTo);
        this.compress = compress;
        this.compressThreshold = compressThreshold;
        this.minhashAnalyzer = minhashAnalyzer;
        this.copyBitsTo = copyBitsTo;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("minhash");
    }

    @Override
    public Object valueForSearch(final Object value) {
        return value(value);
    }

    @Override
    public BytesReference value(final Object value) {
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
            } catch (final IOException e) {
                throw new ElasticsearchParseException(
                        "failed to convert bytes", e);
            }
        }
        try {
            return CompressorFactory.uncompressIfNeeded(bytes);
        } catch (final IOException e) {
            throw new ElasticsearchParseException(
                    "failed to decompress source", e);
        }
    }

    @Override
    protected void parseCreateField(final ParseContext context,
            final List<Field> fields) throws IOException {
        if (!fieldType().stored() && !hasDocValues()) {
            return;
        }
        String text = context.parseExternalValue(String.class);
        if (text == null) {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                text = context.parser().textOrNull();
            }
        }
        if (text == null) {
            return;
        }

        byte[] value = MinHash.calculate(minhashAnalyzer, text);
        if (value == null) {
            return;
        }

        if (copyBitsTo != null) {
            final CopyBitsTo.Builder builder = new CopyBitsTo.Builder();
            builder.add(copyBitsTo);
            final CopyBitsTo copyBitsTo = builder.build();
            copyBitsTo.parse(context.createExternalValueContext(MinHash
                    .toBinaryString(value)));
        }

        if (compress != null && compress
                && !CompressorFactory.isCompressed(value, 0, value.length)) {
            if (compressThreshold == -1 || value.length > compressThreshold) {
                final BytesStreamOutput bStream = new BytesStreamOutput();
                final StreamOutput stream = CompressorFactory
                        .defaultCompressor().streamOutput(bStream);
                stream.writeBytes(value, 0, value.length);
                stream.close();
                value = bStream.bytes().toBytes();
            }
        }
        if (fieldType().stored()) {
            fields.add(new Field(names.indexName(), value, fieldType));
        }

        if (hasDocValues()) {
            CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context
                    .doc().getByKey(names().indexName());
            if (field == null) {
                field = new CustomBinaryDocValuesField(names().indexName(),
                        value);
                context.doc().addWithKey(names().indexName(), field);
            } else {
                field.add(value);
            }
        }

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(final XContentBuilder builder,
            final boolean includeDefaults, final Params params)
            throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("minhash_analyzer", minhashAnalyzer.name());
        builder.field("copy_bits_to", copyBitsTo);
        if (compress != null) {
            builder.field("compress", compress);
        } else if (includeDefaults) {
            builder.field("compress", false);
        }
        if (compressThreshold != -1) {
            builder.field("compress_threshold", new ByteSizeValue(
                    compressThreshold).toString());
        } else if (includeDefaults) {
            builder.field("compress_threshold", -1);
        }
    }

    @Override
    public void merge(final Mapper mergeWith, final MergeContext mergeContext)
            throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }

        final MinHashFieldMapper sourceMergeWith = (MinHashFieldMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            if (sourceMergeWith.compress != null) {
                compress = sourceMergeWith.compress;
            }
            if (sourceMergeWith.compressThreshold != -1) {
                compressThreshold = sourceMergeWith.compressThreshold;
            }
        }
    }

    public static class CustomBinaryDocValuesField extends
            NumberFieldMapper.CustomNumericDocValuesField {

        public static final FieldType TYPE = new FieldType();
        static {
            TYPE.setDocValueType(FieldInfo.DocValuesType.BINARY);
            TYPE.freeze();
        }

        private final ObjectArrayList<byte[]> bytesList;

        private int totalSize = 0;

        public CustomBinaryDocValuesField(final String name, final byte[] bytes) {
            super(name);
            bytesList = new ObjectArrayList<>();
            add(bytes);
        }

        public void add(final byte[] bytes) {
            bytesList.add(bytes);
            totalSize += bytes.length;
        }

        @Override
        public BytesRef binaryValue() {
            try {
                CollectionUtils.sortAndDedup(bytesList);
                final int size = bytesList.size();
                final byte[] bytes = new byte[totalSize + (size + 1) * 5];
                final ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
                out.writeVInt(size); // write total number of values
                for (int i = 0; i < size; i++) {
                    final byte[] value = bytesList.get(i);
                    final int valueLength = value.length;
                    out.writeVInt(valueLength);
                    out.writeBytes(value, 0, valueLength);
                }
                return new BytesRef(bytes, 0, out.getPosition());
            } catch (final IOException e) {
                throw new ElasticsearchException("Failed to get binary value",
                        e);
            }

        }
    }

    public static class CopyBitsTo {

        private final ImmutableList<String> copyBitsToFields;

        private CopyBitsTo(final ImmutableList<String> copyToFields) {
            copyBitsToFields = copyToFields;
        }

        /**
         * Creates instances of the fields that the current field should be copied to
         */
        public void parse(final ParseContext context) throws IOException {
            for (final String field : copyBitsToFields) {
                parse(field, context);
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
            private final ImmutableList.Builder<String> copyToBuilders = ImmutableList
                    .builder();

            public Builder add(final String field) {
                copyToBuilders.add(field);
                return this;
            }

            public CopyBitsTo build() {
                return new CopyBitsTo(copyToBuilders.build());
            }
        }

        public ImmutableList<String> copyToFields() {
            return copyBitsToFields;
        }

        /**
         * Creates an copy of the current field with given field name and boost
         */
        public void parse(final String field, final ParseContext context)
                throws IOException {
            final FieldMappers mappers = context.docMapper().mappers()
                    .indexName(field);
            if (mappers != null && !mappers.isEmpty()) {
                mappers.mapper().parse(context);
            } else {
                final int posDot = field.lastIndexOf('.');
                if (posDot > 0) {
                    // Compound name
                    final String objectPath = field.substring(0, posDot);
                    final String fieldPath = field.substring(posDot + 1);
                    final ObjectMapper mapper = context.docMapper()
                            .objectMappers().get(objectPath);
                    if (mapper == null) {
                        //TODO: Create an object dynamically?
                        throw new MapperParsingException(
                                "attempt to copy value to non-existing object ["
                                        + field + "]");
                    }

                    final ContentPath.Type origPathType = context.path()
                            .pathType();
                    context.path().pathType(ContentPath.Type.FULL);
                    context.path().add(objectPath);

                    // We might be in dynamically created field already, so need to clean withinNewMapper flag
                    // and then restore it, so we wouldn't miss new mappers created from copy_to fields
                    final boolean origWithinNewMapper = context
                            .isWithinNewMapper();
                    context.clearWithinNewMapper();

                    try {
                        mapper.parseDynamicValue(context, fieldPath, context
                                .parser().currentToken());
                    } finally {
                        if (origWithinNewMapper) {
                            context.setWithinNewMapper();
                        } else {
                            context.clearWithinNewMapper();
                        }
                        context.path().remove();
                        context.path().pathType(origPathType);
                    }

                } else {
                    // We might be in dynamically created field already, so need to clean withinNewMapper flag
                    // and then restore it, so we wouldn't miss new mappers created from copy_to fields
                    final boolean origWithinNewMapper = context
                            .isWithinNewMapper();
                    context.clearWithinNewMapper();
                    try {
                        context.docMapper()
                                .root()
                                .parseDynamicValue(context, field,
                                        context.parser().currentToken());
                    } finally {
                        if (origWithinNewMapper) {
                            context.setWithinNewMapper();
                        } else {
                            context.clearWithinNewMapper();
                        }
                    }

                }
            }
        }

    }
}