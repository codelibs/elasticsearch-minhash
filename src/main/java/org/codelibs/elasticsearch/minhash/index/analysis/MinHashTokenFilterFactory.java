package org.codelibs.elasticsearch.minhash.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettings;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private int hashBit;

    private HashFunction[] hashFunctions;

    @Inject
    public MinHashTokenFilterFactory(Index index,
            @IndexSettings Settings indexSettings, @Assisted String name,
            @Assisted Settings settings) {
        super(index, indexSettings, name, settings);

        hashBit = settings.getAsInt("b", 1);
        int numOfHash = settings.getAsInt("k", 128);
        int seed = settings.getAsInt("seed", 0);

        hashFunctions = createHashFunctions(seed, numOfHash);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Index:{} -> {}-bit minhash with {} murmur3({}) functions.",
                    index.name(), hashBit, numOfHash, seed);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new MinHashTokenFilter(tokenStream, hashFunctions, hashBit);
    }

    public static HashFunction[] createHashFunctions(int seed, int num) {
        HashFunction[] hashFunctions = new HashFunction[num];
        for (int i = 0; i < num; i++) {
            hashFunctions[i] = Hashing.murmur3_128(seed + i);
        }
        return hashFunctions;
    }
}
