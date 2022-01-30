/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.elasticsearch.minhash.index.mapper;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.isArray;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentParser;

public class MinHashFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "minhash";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    public static class MinHashField extends Field {
        public MinHashField(final String field, final CharSequence term,
                final FieldType ft) {
            super(field, term, ft);
        }
    }

    private static MinHashFieldMapper toType(final FieldMapper in) {
        return (MinHashFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter
                .indexParam(m -> toType(m).indexed, true);

        private final Parameter<Boolean> hasDocValues = Parameter
                .docValuesParam(m -> toType(m).hasDocValues, true);

        private final Parameter<Boolean> stored = Parameter
                .storeParam(m -> toType(m).stored, false);

        private final Parameter<String> nullValue = Parameter.stringParam(
                "null_value", false, m -> toType(m).nullValue, null);

        private final Parameter<Boolean> bitString = Parameter.boolParam(
                "bit_string", false, m -> toType(m).bitString, false);

        private final Parameter<Map<String, String>> meta = Parameter
                .metaParam();

        private final Parameter<String> minhashAnalyzer = Parameter
                .stringParam("minhash_analyzer", true, m -> {
                    final NamedAnalyzer minhashAnalyzer = toType(
                            m).minhashAnalyzer;
                    if (minhashAnalyzer != null) {
                        return minhashAnalyzer.name();
                    }
                    return "standard";
                }, "standard");

        @Deprecated
        private final Parameter<String[]> copyBitsTo = new Parameter<>(
                "copy_bits_to", true, () -> new String[0],
                (n, c, o) -> parseCopyBitsFields(o), m -> new String[0]);

        private final MappingParserContext parserContext;

        private NamedAnalyzer mergedAnalyzer;

        public Builder(final String name,
                final MappingParserContext parserContext) {
            super(name);
            this.parserContext = parserContext;
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(meta, indexed, stored, hasDocValues, nullValue,
                    bitString, minhashAnalyzer, copyBitsTo);
        }

        @Override
        public Builder init(final FieldMapper initializer) {
            super.init(initializer);
            if (initializer instanceof MinHashFieldMapper) {
                final MinHashFieldMapper mapper = (MinHashFieldMapper) initializer;
                this.indexed.setValue(mapper.indexed);
                this.hasDocValues.setValue(mapper.hasDocValues);
                this.nullValue.setValue(mapper.nullValue);
                this.bitString.setValue(mapper.bitString);
                this.mergedAnalyzer = mapper.minhashAnalyzer;
            }
            return this;
        }

        public Builder minhashAnalyzer(final NamedAnalyzer minhashAnalyzer) {
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

        private MinHashFieldType buildFieldType(
                final MapperBuilderContext context, final FieldType fieldType) {
            return new MinHashFieldType(context.buildFullName(name), fieldType,
                    indexed.getValue(), stored.getValue(),
                    hasDocValues.getValue(), meta.getValue());
        }

        @Override
        public MinHashFieldMapper build(final MapperBuilderContext context) {
            final FieldType fieldtype = new FieldType(
                    MinHashFieldMapper.Defaults.FIELD_TYPE);
            fieldtype.setIndexOptions(
                    indexed.getValue() ? IndexOptions.DOCS : IndexOptions.NONE);
            fieldtype.setStored(this.stored.getValue());
            return new MinHashFieldMapper(name, fieldtype,
                    buildFieldType(context, fieldtype),
                    multiFieldsBuilder.build(this, context), copyTo.build(),
                    this, minhashAnalyzer());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public MinHashFieldMapper.Builder parse(final String name,
                final Map<String, Object> node,
                final MappingParserContext parserContext)
                throws MapperParsingException {
            final MinHashFieldMapper.Builder builder = new MinHashFieldMapper.Builder(
                    name, parserContext);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

    @Deprecated
    public static String[] parseCopyBitsFields(final Object propNode) {
        if (isArray(propNode)) {
            @SuppressWarnings("unchecked")
            final List<Object> nodeList = (List<Object>) propNode;
            return nodeList.stream().map(o -> nodeStringValue(o, null))
                    .filter(s -> s != null).toArray(n -> new String[n]);
        }
        return new String[] { nodeStringValue(propNode, null) };
    }

    public static final class MinHashFieldType extends StringFieldType {
        public MinHashFieldType(final String name, final FieldType fieldType,
                final boolean isIndexed, final boolean isStored,
                final boolean hasDocValues, final Map<String, String> meta) {
            super(name, isIndexed, isStored, hasDocValues,
                    new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER,
                            Lucene.KEYWORD_ANALYZER),
                    meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(final SearchExecutionContext context,
                final String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(
                final String fullyQualifiedIndexName,
                final Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(),
                    CoreValuesSourceType.KEYWORD);
        }

        @Override
        public CollapseType collapseType() {
            return CollapseType.KEYWORD;
        }
    }

    private final boolean indexed;

    private final boolean stored;

    private final boolean hasDocValues;

    private final String nullValue;

    private final boolean bitString;

    private final NamedAnalyzer minhashAnalyzer;

    private final FieldType fieldType;

    protected MinHashFieldMapper(final String simpleName,
            final FieldType fieldType, final MappedFieldType mappedFieldType,
            final MultiFields multiFields, final CopyTo copyTo,
            final Builder builder, final NamedAnalyzer minhashAnalyzer) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.indexed = builder.indexed.getValue();
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.bitString = builder.bitString.getValue();
        this.minhashAnalyzer = minhashAnalyzer;
        this.fieldType = fieldType;
    }

    @Override
    protected void parseCreateField(final DocumentParserContext context)
            throws IOException {
        if (!indexed && !stored && !hasDocValues) {
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
        final String stringValue;
        if (bitString) {
            stringValue = MinHash.toBinaryString(minhashValue);
        } else {
            stringValue = new String(Base64.getEncoder().encode(minhashValue),
                    StandardCharsets.UTF_8);
        }

        if (indexed || stored) {
            final IndexableField field = new MinHashField(fieldType().name(),
                    stringValue, fieldType);
            context.doc().add(field);

            if (!hasDocValues) {
                context.addToFieldNames(fieldType().name());
            }
        }

        if (hasDocValues) {
            final BytesRef binaryValue = new BytesRef(stringValue);
            context.doc().add(new SortedSetDocValuesField(fieldType().name(),
                    binaryValue));
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new MinHashFieldMapper.Builder(simpleName(), null).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
