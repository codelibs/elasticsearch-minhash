package org.codelibs.elasticsearch.minhash.index.mapper;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;

public class RegisterMinHashType extends AbstractIndexComponent {

    @Inject
    public RegisterMinHashType(final Index index,
            @IndexSettings final Settings indexSettings,
            final MapperService mapperService) {
        super(index, indexSettings);

        mapperService.documentMapperParser().putTypeParser("minhash",
                new MinHashFieldMapper.TypeParser());
    }

}
