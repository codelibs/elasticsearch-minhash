package org.codelibs.elasticsearch.minhash.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.codelibs.minhash.MinHash;
import org.codelibs.minhash.analysis.MinHashTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

import com.google.common.hash.HashFunction;

public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int hashBit;

    private final HashFunction[] hashFunctions;

    public MinHashTokenFilterFactory(final IndexSettings indexSettings, final Environment environment, final String name, final Settings settings) {
        super(indexSettings, name, settings);

        hashBit = settings.getAsInt("bit", 1);
        final int numOfHash = settings.getAsInt("size", 128);
        final int seed = settings.getAsInt("seed", 0);

        hashFunctions = MinHash.createHashFunctions(seed, numOfHash);

        if (logger.isDebugEnabled()) {
            logger.debug("Index:{} -> {}-bit minhash with {} murmur3({}) functions.", indexSettings.getIndex(), hashBit, numOfHash, seed);
        }
    }

    @Override
    public TokenStream create(final TokenStream tokenStream) {
        return new MinHashTokenFilter(tokenStream, hashFunctions, hashBit);
    }
}
