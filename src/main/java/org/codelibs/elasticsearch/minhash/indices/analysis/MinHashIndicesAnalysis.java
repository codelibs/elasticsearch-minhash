package org.codelibs.elasticsearch.minhash.indices.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.codelibs.minhash.MinHash;
import org.codelibs.minhash.analysis.MinHashTokenFilter;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

public class MinHashIndicesAnalysis extends AbstractComponent {
    @Inject
    public MinHashIndicesAnalysis(final Settings settings,
            final IndicesAnalysisService indicesAnalysisService) {
        super(settings);

        indicesAnalysisService.tokenFilterFactories().put("minhash",
                new MinHashTokenFilterFactoryFactory(new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return "minhash";
                    }

                    @Override
                    public TokenStream create(final TokenStream tokenStream) {
                        return new MinHashTokenFilter(tokenStream, MinHash
                                .createHashFunctions(0, 128), 1);
                    }
                }));
    }
}
