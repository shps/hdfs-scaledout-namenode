
package org.apache.hadoop.hdfs.util;

import java.io.IOException;


/**
 * This interface makes @GSet to support the option showing if a call to its methods are
 * already a part of a transaction or not.
 * @author Hooman <hooman@sics.se>
 */
public interface GSetDB<K, E extends K> extends GSet<K, E> {
    
    /**
     * 
     * @param element
     * @param isTransactional
     * @return 
     */
    E put(E element, boolean isTransactional) throws IOException;
   
    /**
     * 
     * @param key
     * @param isTransactional
     * @return 
     */
    E remove(K key, boolean isTransactional) throws IOException;
}
