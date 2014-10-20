package org.codelibs.elasticsearch.minhash;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.codelibs.elasticsearch.minhash.index.analysis.MinHashTokenFilterFactory;
import org.codelibs.elasticsearch.minhash.module.MinHashAnalysisModule;
import org.codelibs.elasticsearch.minhash.module.MinHashIndexModule;
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

    public void onModule(final AnalysisModule module) {
        module.addTokenFilter("minhash", MinHashTokenFilterFactory.class);
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        final Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(MinHashIndexModule.class);
        return modules;
    }
}
