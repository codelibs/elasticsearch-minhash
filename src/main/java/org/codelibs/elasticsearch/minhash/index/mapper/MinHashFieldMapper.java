package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

import java.io.IOException;
import java.io.Reader;
import java.util.Base64;
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
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import com.carrotsearch.hppc.ObjectArrayList;

public class MinHashFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "minhash";
    private NamedAnalyzer minhashAnalyzer;
    private String copyBitsTo;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new MinHashFieldType();

        static {
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
            MinHashFieldMapper.Builder builder = new MinHashFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("minhash_analyzer")
                        && propNode != null) {
                    final NamedAnalyzer analyzer = parserContext
                            .getIndexAnalyzers().get(propNode.toString());
                    builder.minhashAnalyzer(analyzer);
                    iterator.remove();
                } else if (propName.equals("copy_bits_to")
                        && propNode != null) {
                    builder.copyBitsTo(propNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class MinHashFieldType extends MappedFieldType {

        public MinHashFieldType() {}

        protected MinHashFieldType(MinHashFieldType ref) {
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
        public BytesReference valueForDisplay(Object value) {
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
                bytes = new BytesArray(Base64.getDecoder().decode(value.toString()));
            }
            return bytes;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new BytesBinaryDVIndexFieldData.Builder();
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "MinHash fields do not support searching");
        }
    }

    protected MinHashFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo,
                                NamedAnalyzer minhashAnalyzer, String copyBitsTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.minhashAnalyzer = minhashAnalyzer;
        this.copyBitsTo = copyBitsTo;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!fieldType().stored() && !fieldType().hasDocValues()) {
            return;
        }
        byte[] value = context.parseExternalValue(byte[].class);
        if (value == null) {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                value = context.parser().binaryValue();
            }
        }
        if (value == null) {
            return;
        }
        byte[] minhashValue = MinHash.calculate(minhashAnalyzer, new String(value, "UTF-8"));
        if (fieldType().stored()) {
            fields.add(new Field(fieldType().name(), minhashValue, fieldType()));
        }

        if (fieldType().hasDocValues()) {
            CustomMinHashDocValuesField field = (CustomMinHashDocValuesField) context.doc().getByKey(fieldType().name());
            if (field == null) {
                field = new CustomMinHashDocValuesField(fieldType().name(), minhashValue);
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                field.add(minhashValue);
            }
        }

        // TODO
//        if (copyBitsTo != null) {
//            final CopyBitsTo.Builder builder = new CopyBitsTo.Builder(parseCopyMethod);
//            builder.add(copyBitsTo);
//            final CopyBitsTo copyBitsTo = builder.build();
//            copyBitsTo.parse(context.createExternalValueContext(MinHash
//                    .toBinaryString(value)));
//        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
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

        public CustomMinHashDocValuesField(String name, byte[] bytes) {
            this.name = name;
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

        @Override
        public String name() {
            return name;
        }

        @Override
        public IndexableFieldType fieldType() {
            return TYPE;
        }

        @Override
        public float boost() {
            return 1f;
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
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            return null;
        }
    }
}

