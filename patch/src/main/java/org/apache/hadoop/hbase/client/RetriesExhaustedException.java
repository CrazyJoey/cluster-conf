package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;

/**
 * Created by jh4j on 2016/9/8.
 */
public class RetriesExhaustedException extends IOException {


    private static final long serialVersionUID = 1876775844L;

    public RetriesExhaustedException(final String msg){
        super(msg);
    }

    public RetriesExhaustedException(final String msg, final IOException e){
        super(msg, e);
    }

}
