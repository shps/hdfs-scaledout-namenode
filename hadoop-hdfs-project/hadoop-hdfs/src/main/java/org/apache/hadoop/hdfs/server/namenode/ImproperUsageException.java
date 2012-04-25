package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

/**
 *  This exception is targeted toward improper usage by client callers in terms of
 *  writing oprerations that called on a reading NN.
 * 
 * @author Kamal Hakimzadeh <kamal@sics.se>
 */
public class ImproperUsageException extends IOException{

    public ImproperUsageException() {
        super("Calling the write operations is not allowed inside Non-Writing NN");
    }

    public ImproperUsageException(String msg) {
        super(msg);
    }
    
}
