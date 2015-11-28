package org.codelibs.elasticsearch.minhash.index.mapper;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsService;

public class RegisterMinHashType extends AbstractIndexComponent {

    @Inject
    public RegisterMinHashType(final Index index,
            final IndexSettingsService indexSettingsService,
            final MapperService mapperService) {
        super(index, indexSettingsService.getSettings());

        mapperService.documentMapperParser().putTypeParser("minhash",
                new MinHashFieldMapper.TypeParser());
    }

}
