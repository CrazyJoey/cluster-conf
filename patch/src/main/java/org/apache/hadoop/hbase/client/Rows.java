package org.apache.hadoop.hbase.client;

/**
 * Created by jh4j on 2016/9/9.
 */
public interface Rows extends Comparable<Row> {

    /**
     * @return The row.
     */
    byte[] getRow();
}
