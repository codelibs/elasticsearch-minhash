package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.isArray;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import com.carrotsearch.hppc.ObjectArrayList;

public class MinHashFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "minhash";

    private NamedAnalyzer minhashAnalyzer;

    private CopyBitsTo copyBitsTo;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new MinHashFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder
            extends FieldMapper.Builder<Builder, MinHashFieldMapper> {

        private NamedAnalyzer minhashAnalyzer;

        private CopyBitsTo copyBitsTo;

        public Builder(final String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public MinHashFieldMapper build(final BuilderContext context) {
            setupFieldType(context);
            return new MinHashFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo,
                    minhashAnalyzer, copyBitsTo);
        }

        public Builder minhashAnalyzer(final NamedAnalyzer minhashAnalyzer) {
            this.minhashAnalyzer = minhashAnalyzer;
            return builder;
        }

        public Builder copyBitsTo(final CopyBitsTo copyBitsTo) {
            this.copyBitsTo = copyBitsTo;
            return builder;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(final String name, final Map<String, Object> node,
                final ParserContext parserContext) throws MapperParsingException {
            final MinHashFieldMapper.Builder builder = new MinHashFieldMapper.Builder(
                    name);
            parseField(builder, name, node, parserContext);
            for (final Iterator<Map.Entry<String, Object>> iterator = node.entrySet()
                    .iterator(); iterator.hasNext();) {
                final Map.Entry<String, Object> entry = iterator.next();
                final String propName = entry.getKey();
                final Object propNode = entry.getValue();
                if (propName.equals("minhash_analyzer") && propNode != null) {
                    final NamedAnalyzer analyzer = parserContext
                            .getIndexAnalyzers().get(propNode.toString());
                    builder.minhashAnalyzer(analyzer);
                    iterator.remove();
                } else if (propName.equals("copy_bits_to")
                        && propNode != null) {
                    parseCopyBitsFields(propNode, builder);
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static void parseCopyBitsFields(final Object propNode, final Builder builder) {
        final CopyBitsTo.Builder copyToBuilder = new CopyBitsTo.Builder();
        if (isArray(propNode)) {
            for (final Object node : (List<Object>) propNode) {
                copyToBuilder.add(nodeStringValue(node, null));
            }
        } else {
            copyToBuilder.add(nodeStringValue(propNode, null));
        }
        builder.copyBitsTo(copyToBuilder.build());
    }

    static final class MinHashFieldType extends MappedFieldType {

        public MinHashFieldType() {
        }

        protected MinHashFieldType(final MinHashFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new MinHashFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
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
            return new BytesBinaryDVIndexFieldData.Builder();
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

    protected MinHashFieldMapper(final String simpleName, final MappedFieldType fieldType,
            final MappedFieldType defaultFieldType, final Settings indexSettings,
            final MultiFields multiFields, final CopyTo copyTo,
            final NamedAnalyzer minhashAnalyzer, final CopyBitsTo copyBitsTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings,
                multiFields, copyTo);
        this.minhashAnalyzer = minhashAnalyzer;
        this.copyBitsTo = copyBitsTo;
    }

    @Override
    protected void parseCreateField(final ParseContext context,
            final List<IndexableField> fields) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            final XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = fieldType().nullValueAsString();
            } else {
                value = parser.textOrNull();
            }
        }

        if (value == null) {
            return;
        }

        final byte[] minhashValue = MinHash.calculate(minhashAnalyzer, value);
        if (fieldType().stored()) {
            fields.add(
                    new Field(fieldType().name(), minhashValue, fieldType()));
        }

        if (fieldType().hasDocValues()) {
            CustomMinHashDocValuesField field = (CustomMinHashDocValuesField) context
                    .doc().getByKey(fieldType().name());
            if (field == null) {
                field = new CustomMinHashDocValuesField(fieldType().name(),
                        minhashValue);
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                field.add(minhashValue);
            }
        }

        if (copyBitsTo != null) {
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
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(final Mapper mergeWith, final boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        this.minhashAnalyzer = ((MinHashFieldMapper) mergeWith).minhashAnalyzer;
        this.copyBitsTo = ((MinHashFieldMapper) mergeWith).copyBitsTo;
    }

    @Override
    protected void doXContentBody(final XContentBuilder builder,
            final boolean includeDefaults, final Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("minhash_analyzer", minhashAnalyzer.name());
        if (copyBitsTo != null) {
            copyBitsTo.toXContent(builder, params);
        }
    }

    public static class CustomMinHashDocValuesField implements IndexableField {

        public static final FieldType TYPE = new FieldType();
        static {
            TYPE.setDocValuesType(DocValuesType.BINARY);
            TYPE.freeze();
        }

        private final ObjectArrayList<byte[]> bytesList;

        private int totalSize = 0;

        private final String name;

        public CustomMinHashDocValuesField(final String name, final byte[] bytes) {
            this.name = name;
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
                throw new ElasticsearchException("Failed to get MinHash value",
                        e);
            }

        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public IndexableFieldType fieldType() {
            return TYPE;
        }

        @Override
        public String stringValue() {
            return null;
        }

        @Override
        public Reader readerValue() {
            return null;
        }

        @Override
        public Number numericValue() {
            return null;
        }

        @Override
        public TokenStream tokenStream(final Analyzer analyzer, final TokenStream reuse) {
            return null;
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
