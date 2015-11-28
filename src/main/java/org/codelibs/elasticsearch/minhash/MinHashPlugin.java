package org.codelibs.elasticsearch.minhash;

import java.util.ArrayList;
import java.util.Collection;

import org.codelibs.elasticsearch.minhash.index.analysis.MinHashTokenFilterFactory;
import org.codelibs.elasticsearch.minhash.module.MinHashAnalysisModule;
import org.codelibs.elasticsearch.minhash.module.MinHashIndexModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.plugins.Plugin;

public class MinHashPlugin extends Plugin {
    @Override
    public String name() {
        return "MinHashPlugin";
    }

    @Override
    public String description() {
        return "This plugin provides b-bit minhash algorism.";
    }

    @Override
    public Collection<Module> nodeModules() {
        ArrayList<Module> modules = new ArrayList<>();
        modules.add(new MinHashAnalysisModule());
        return modules;
    }

    public void onModule(final AnalysisModule module) {
        module.addTokenFilter("minhash", MinHashTokenFilterFactory.class);
    }

    @Override
    public Collection<Module> indexModules(Settings indexSettings) {
        final Collection<Module> modules = new ArrayList<>();
        modules.add(new MinHashIndexModule());
        return modules;
    }
}
