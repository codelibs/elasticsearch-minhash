package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.isArray;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.CustomDocValuesField;
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.Mapper.TypeParser.ParserContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import com.carrotsearch.hppc.ObjectArrayList;

public class MinHashFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "minhash";
    
    private static MinHashFieldMapper toType(FieldMapper in) {
        return (MinHashFieldMapper) in;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> stored = Parameter.boolParam("store", false, m -> toType(m).stored, true);
        private final Parameter<Boolean> hasDocValues = Parameter.boolParam("doc_values", false, m -> toType(m).hasDocValues,  false);
        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m->toType(m).nullValue, null);
        private final Parameter<Map<String, String>> meta
            = new Parameter<>("meta", true, Collections.emptyMap(), TypeParsers::parseMeta, m -> m.fieldType().meta());
        private final Parameter<String> minhashAnalyzer = Parameter.stringParam(
                "minhash_analyzer", true,
                m -> toType(m).minhashAnalyzer.name(), null);
        private final Parameter<String[]> copyBitsTo = new Parameter<>(
                "copy_bits_to", true, null, (n, o) -> parseCopyBitsFields(o),
                m -> {
                    List<String> fieldList = toType(m).copyBitsTo
                            .copyBitsToFields();
                    return fieldList.toArray(new String[fieldList.size()]);
                });
        private ParserContext parserContext;

        public Builder(String name) {
            this(name, null, false);
        }

        public Builder(String name, ParserContext parserContext, boolean hasDocValues) {
            super(name);
            this.parserContext = parserContext;
            this.hasDocValues.setValue(hasDocValues);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(meta, stored, hasDocValues, nullValue, minhashAnalyzer, copyBitsTo);
        }

        @Override
        public Builder init(FieldMapper initializer) {
            super.init(initializer);
            return this;
        }

        @Override
        public MinHashFieldMapper build(BuilderContext context) {
            return new MinHashFieldMapper(name, new MinHashFieldType(buildFullName(context), hasDocValues.getValue(), meta.getValue()),
                    multiFieldsBuilder.build(this, context), copyTo.build(), this,
                    minhashAnalyzer.getValue(), copyBitsTo.getValue(), parserContext);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public MinHashFieldMapper.Builder parse(final String name, final Map<String, Object> node,
                final ParserContext parserContext) throws MapperParsingException {
            final MinHashFieldMapper.Builder builder = new MinHashFieldMapper.Builder(
                    name, parserContext, false);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

    public static String[] parseCopyBitsFields(final Object propNode) {
        if (isArray(propNode)) {
            @SuppressWarnings("unchecked")
            final List<Object> nodeList = (List<Object>) propNode;
            return nodeList.stream().map(o -> nodeStringValue(o, null))
                    .filter(s -> s != null).toArray(n -> new String[n]);
        } else {
            return new String[] { nodeStringValue(propNode, null) };
        }
    }

    static final class MinHashFieldType extends MappedFieldType {

        public MinHashFieldType(String name, boolean hasDocValues, Map<String, String> meta) {
            super(name, false, hasDocValues, TextSearchInfo.NONE, meta);
        }

        public MinHashFieldType(String name) {
            this(name, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }

        @Override
        public BytesReference valueForDisplay(final Object value) {
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
                bytes = new BytesArray(
                        Base64.getDecoder().decode(value.toString()));
            }
            return bytes;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(CoreValuesSourceType.BYTES);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(final Object value, final QueryShardContext context) {
            throw new QueryShardException(context,
                    "MinHash fields do not support searching");
        }
    }

    private final boolean stored;
    private final boolean hasDocValues;
    private final String nullValue;
    private NamedAnalyzer minhashAnalyzer;
    private CopyBitsTo copyBitsTo;

    protected MinHashFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                MultiFields multiFields, CopyTo copyTo, Builder builder,
                String minhashAnalyzer, String[] copyBitsTo, ParserContext parserContext) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.nullValue = builder.nullValue.getValue();
        if (parserContext != null) {
            this.minhashAnalyzer = parserContext.getIndexAnalyzers()
                    .get(minhashAnalyzer);
        }
        final CopyBitsTo.Builder copyToBuilder = new CopyBitsTo.Builder();
        if (copyBitsTo != null && copyBitsTo.length > 0) {
            for (final String value : copyBitsTo) {
                copyToBuilder.add(value);
            }
        }
        this.copyBitsTo = copyToBuilder.build();
    }

    @Override
    protected void parseCreateField(final ParseContext context) throws IOException {
        if (stored == false && hasDocValues == false) {
            return;
        }
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            final XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = nullValue;
            } else {
                value = parser.textOrNull();
            }
        }

        if (value == null) {
            return;
        }

        final byte[] minhashValue = MinHash.calculate(minhashAnalyzer, value);
        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), minhashValue));
        }

        if (hasDocValues) {
            CustomMinHashDocValuesField field = (CustomMinHashDocValuesField) context
                    .doc().getByKey(fieldType().name());
            if (field == null) {
                field = new CustomMinHashDocValuesField(fieldType().name(),
                        minhashValue);
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                field.add(minhashValue);
            }
        } else {
            // Only add an entry to the field names field if the field is stored
            // but has no doc values so exists query will work on a field with
            // no doc values
            createFieldNamesField(context);
        }

        if (!copyBitsTo.copyBitsToFields().isEmpty()) {
            parseCopyBitsFields(
                    context.createExternalValueContext(
                            MinHash.toBinaryString(minhashValue)),
                    copyBitsTo.copyBitsToFields);
        }
    }

    /** Creates instances of the fields that the current field should be copied to */
    private static void parseCopyBitsFields(ParseContext context,
            final List<String> copyToFields) throws IOException {
        if (!context.isWithinCopyTo() && copyToFields.isEmpty() == false) {
            context = context.createCopyToContext();
            for (final String field : copyToFields) {
                // In case of a hierarchy of nested documents, we need to figure out
                // which document the field should go to
                ParseContext.Document targetDoc = null;
                for (ParseContext.Document doc = context
                        .doc(); doc != null; doc = doc.getParent()) {
                    if (field.startsWith(doc.getPrefix())) {
                        targetDoc = doc;
                        break;
                    }
                }
                assert targetDoc != null;
                final ParseContext copyToContext;
                if (targetDoc == context.doc()) {
                    copyToContext = context;
                } else {
                    copyToContext = context.switchDoc(targetDoc);
                }
                parseCopy(field, copyToContext);
            }
        }
    }

    /** Creates an copy of the current field with given field name and boost */
    private static void parseCopy(final String field, final ParseContext context)
            throws IOException {
        Mapper mapper = context.docMapper().mappers().getMapper(field);
        if (mapper != null) {
            if (mapper instanceof FieldMapper) {
                ((FieldMapper) mapper).parse(context);
            } else if (mapper instanceof FieldAliasMapper) {
                throw new IllegalArgumentException("Cannot copy to a field alias [" + mapper.name() + "].");
            } else {
                throw new IllegalStateException("The provided mapper [" + mapper.name() +
                    "] has an unrecognized type [" + mapper.getClass().getSimpleName() + "].");
            }
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new MinHashFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class CustomMinHashDocValuesField extends CustomDocValuesField {

        private final ObjectArrayList<byte[]> bytesList;

        private int totalSize = 0;

        public CustomMinHashDocValuesField(String name, byte[] bytes) {
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
                throw new ElasticsearchException("Failed to get MinHash value", e);
            }

        }
    }

    public static class CopyBitsTo {

        private final List<String> copyBitsToFields;

        private CopyBitsTo(final List<String> copyBitsToFields) {
            this.copyBitsToFields = copyBitsToFields;
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
            private final List<String> copyBitsToBuilders = new ArrayList<>();

            public Builder add(final String field) {
                copyBitsToBuilders.add(field);
                return this;
            }

            public CopyBitsTo build() {
                return new CopyBitsTo(
                        Collections.unmodifiableList(copyBitsToBuilders));
            }
        }

        public List<String> copyBitsToFields() {
            return copyBitsToFields;
        }
    }
}
