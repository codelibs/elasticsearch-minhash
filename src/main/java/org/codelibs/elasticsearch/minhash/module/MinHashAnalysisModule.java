package org.codelibs.elasticsearch.minhash.module;

import org.codelibs.elasticsearch.minhash.indices.analysis.MinHashIndicesAnalysis;
import org.elasticsearch.common.inject.AbstractModule;

public class MinHashAnalysisModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MinHashIndicesAnalysis.class).asEagerSingleton();
    }
}