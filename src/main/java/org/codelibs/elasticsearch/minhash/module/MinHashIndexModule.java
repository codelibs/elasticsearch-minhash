package org.codelibs.elasticsearch.minhash.module;

import org.codelibs.elasticsearch.minhash.index.mapper.RegisterMinHashType;
import org.elasticsearch.common.inject.AbstractModule;

public class MinHashIndexModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RegisterMinHashType.class).asEagerSingleton();
    }
}
