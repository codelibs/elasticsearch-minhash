package org.codelibs.elasticsearch.minhash;

public class MinHashException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MinHashException(final String message) {
        super(message);
    }

    public MinHashException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
