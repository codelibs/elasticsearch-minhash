package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.isArray;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.CustomDocValuesField;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import com.carrotsearch.hppc.ObjectArrayList;

public class MinHashFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "minhash";
    
    private static MinHashFieldMapper toType(FieldMapper in) {
        return (MinHashFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> stored = Parameter.boolParam("store", false, m -> toType(m).stored, true);
        private final Parameter<Boolean> hasDocValues = Parameter.boolParam("doc_values", false, m -> toType(m).hasDocValues,  false);
        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m->toType(m).nullValue, null);
        private final Parameter<Boolean> bitString= Parameter.boolParam("bit_string", false, m -> toType(m).bitString, false);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> minhashAnalyzer = Parameter
                .stringParam("minhash_analyzer", true, m -> {
                    NamedAnalyzer minhashAnalyzer = toType(m).minhashAnalyzer;
                    if (minhashAnalyzer != null) {
                        return minhashAnalyzer.name();
                    }
                    return "standard";
                }, "standard");
        @Deprecated
        private final Parameter<String[]> copyBitsTo = new Parameter<>(
                "copy_bits_to", true, () -> new String[0],
                (n, c, o) -> parseCopyBitsFields(o), m -> {
                    return new String[0];
                });
        private MappingParserContext parserContext;
        private NamedAnalyzer mergedAnalyzer;

        public Builder(String name) {
            this(name, null, false);
        }

        public Builder(String name, MappingParserContext parserContext, boolean hasDocValues) {
            super(name);
            this.parserContext = parserContext;
            this.hasDocValues.setValue(hasDocValues);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(meta, stored, hasDocValues, nullValue, bitString, minhashAnalyzer, copyBitsTo);
        }

        @Override
        public Builder init(FieldMapper initializer) {
            super.init(initializer);
            return this;
        }

        public Builder minhashAnalyzer(NamedAnalyzer minhashAnalyzer) {
            this.mergedAnalyzer = minhashAnalyzer;
            return this;
        }

        private NamedAnalyzer minhashAnalyzer() {
            if (mergedAnalyzer != null) {
                return mergedAnalyzer;
            }
            if (parserContext != null) {
                return parserContext.getIndexAnalyzers()
                        .get(minhashAnalyzer.getValue());
            }
            return null;
        }

        @Override
        public MinHashFieldMapper build(ContentPath contentPath) {
            return new MinHashFieldMapper(name,
                    new MinHashFieldType(buildFullName(contentPath),
                            stored.getValue(), hasDocValues.getValue(),
                            meta.getValue()),
                    multiFieldsBuilder.build(this, contentPath), copyTo.build(),
                    this, minhashAnalyzer());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public MinHashFieldMapper.Builder parse(final String name, final Map<String, Object> node,
                final MappingParserContext parserContext) throws MapperParsingException {
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
        public MinHashFieldType(String name, boolean isStored, boolean hasDocValues, Map<String, String> meta) {
            super(name, false, isStored, hasDocValues, TextSearchInfo.NONE,
                    meta);
        }

        public MinHashFieldType(String name) {
            this(name, true, false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(final Object value, final SearchExecutionContext context) {
            throw new QueryShardException(context,
                    "MinHash fields do not support searching");
        }
    }

    private final boolean stored;
    private final boolean hasDocValues;
    private final String nullValue;
    private final boolean bitString;
    private NamedAnalyzer minhashAnalyzer;

    protected MinHashFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                MultiFields multiFields, CopyTo copyTo, Builder builder,
                NamedAnalyzer minhashAnalyzer) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.bitString = builder.bitString.getValue();
        this.minhashAnalyzer = minhashAnalyzer;
    }

    @Override
    protected void parseCreateField(final ParseContext context) throws IOException {
        if (stored == false && hasDocValues == false) {
            return;
        }
        String value;
        final XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            value = nullValue;
        } else {
            value = parser.textOrNull();
        }

        if (value == null) {
            return;
        }

        final byte[] minhashValue = MinHash.calculate(minhashAnalyzer, value);
        if (stored) {
            final IndexableField field;
            if (bitString) {
                final FieldType fieldtype = new FieldType(
                        KeywordFieldMapper.Defaults.FIELD_TYPE);
                fieldtype.setStored(stored);
                field = new Field(fieldType().name(),
                        MinHash.toBinaryString(minhashValue),
                        fieldtype);
            } else {
                field = new StoredField(fieldType().name(), minhashValue);
            }
            context.doc().add(field);
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
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        Builder builder = new MinHashFieldMapper.Builder(simpleName())
                .init(this);
        builder.minhashAnalyzer(this.minhashAnalyzer);
        return builder;
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
}
