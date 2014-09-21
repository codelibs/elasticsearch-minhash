package org.codelibs.elasticsearch.minhash;

import java.util.Collection;

import org.codelibs.elasticsearch.minhash.module.MinHashAnalysisModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class MinHashPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "MinHashPlugin";
    }

    @Override
    public String description() {
        return "This plugin provides b-bit minhash algorism.";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList
                .<Class<? extends Module>> of(MinHashAnalysisModule.class);
    }

    public void onModule(AnalysisModule module) {
        // module.addTokenFilter("kuromoji_part_of_speech", KuromojiPartOfSpeechFilterFactory.class);
    }
}
