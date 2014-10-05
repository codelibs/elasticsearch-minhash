package org.codelibs.elasticsearch.minhash.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.codelibs.minhash.MinHash;
import org.codelibs.minhash.analysis.MinHashTokenFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettings;

import com.google.common.hash.HashFunction;

public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private int hashBit;

    private HashFunction[] hashFunctions;

    @Inject
    public MinHashTokenFilterFactory(final Index index,
            @IndexSettings final Settings indexSettings,
            @Assisted final String name, @Assisted final Settings settings) {
        super(index, indexSettings, name, settings);

        hashBit = settings.getAsInt("bit", 1);
        final int numOfHash = settings.getAsInt("size", 128);
        final int seed = settings.getAsInt("seed", 0);

        hashFunctions = MinHash.createHashFunctions(seed, numOfHash);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Index:{} -> {}-bit minhash with {} murmur3({}) functions.",
                    index.name(), hashBit, numOfHash, seed);
        }
    }

    @Override
    public TokenStream create(final TokenStream tokenStream) {
        return new MinHashTokenFilter(tokenStream, hashFunctions, hashBit);
    }
}
