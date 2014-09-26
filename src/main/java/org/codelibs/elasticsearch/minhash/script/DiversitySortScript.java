package org.codelibs.elasticsearch.minhash.script;

import java.util.Map;

import org.codelibs.elasticsearch.minhash.MinHash;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;

public class DiversitySortScript extends AbstractExecutableScript {
    public static final String SCRIPT_NAME = "dynarank_diversity_sort";

    private Map<String, Object> params;

    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(
                @Nullable final Map<String, Object> params) {
            return new DiversitySortScript(params);
        }
    }

    public DiversitySortScript(final Map<String, Object> params) {
        this.params = params;
    }

    @Override
    public Object run() {
        final InternalSearchHit[] searchHits = (InternalSearchHit[]) params
                .get("searchHits");
        final int length = searchHits.length;
        final InternalSearchHit[] newSearchHits = new InternalSearchHit[length];
        final String minhashField = (String) params.get("minhash_field");
        final float threshold = Float.parseFloat((String) params
                .get("minhash_threshold"));
        int pos = 0;
        for (int i = 0; i < length; i++) {
            boolean exists = false;
            for (int j = 0; j < pos; j++) {
                final InternalSearchHit hit = searchHits[i];
                final SearchHitField field = hit.getFields().get(minhashField);
                final BytesArray value = field.getValue();
                final InternalSearchHit newHit = newSearchHits[j];
                final SearchHitField newField = newHit.getFields().get(
                        minhashField);
                final BytesArray newValue = newField.getValue();
                if (MinHash.compare(value, newValue) > threshold) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                newSearchHits[pos] = searchHits[i];
                searchHits[i] = null;
                pos++;
            }
        }

        if (pos < length) {
            for (int i = 0; i < length; i++) {
                if (searchHits[i] != null) {
                    newSearchHits[pos] = searchHits[i];
                    pos++;
                }
            }
        }
        return newSearchHits;
    }

}
