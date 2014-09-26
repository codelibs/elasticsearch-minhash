package org.codelibs.elasticsearch.minhash;

public class MinHashException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MinHashException(String message) {
        super(message);
    }

    public MinHashException(String message, Throwable cause) {
        super(message, cause);
    }

}
