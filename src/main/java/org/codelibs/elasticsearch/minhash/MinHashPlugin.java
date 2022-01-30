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
package org.codelibs.elasticsearch.minhash;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.minhash.index.analysis.MinHashTokenFilterFactory;
import org.codelibs.elasticsearch.minhash.index.mapper.MinHashFieldMapper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

public class MinHashPlugin extends Plugin
        implements MapperPlugin, AnalysisPlugin {

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        final Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("minhash", MinHashTokenFilterFactory::new);
        return extra;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.<String, Mapper.TypeParser> singletonMap(
                MinHashFieldMapper.CONTENT_TYPE,
                new MinHashFieldMapper.TypeParser());
    }
}
