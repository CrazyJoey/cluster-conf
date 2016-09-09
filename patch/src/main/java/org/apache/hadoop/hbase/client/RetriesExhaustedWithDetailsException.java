package org.apache.hadoop.hbase.client;

import java.util.List;

/**
 * Created by jh4j on 2016/9/8.
 */
public class RetriesExhaustedWithDetailsException extends RetriesExhaustedException {
    List<Throwable> exceptions;
    List<Row> actions;
    List<String> hostnameAndPort;

    public RetriesExhaustedWithDetailsException(List<Throwable> exceptions,
                                                List<Row> actions,
                                                List<String> hostnameAndPort) {
        super("Failed " + exceptions.size() + " action" +
                pluralize(exceptions) + ": " +
                getDesc(exceptions, actions, hostnameAndPort));

        this.exceptions = exceptions;
        this.actions = actions;
        this.hostnameAndPort = hostnameAndPort;
    }
}
