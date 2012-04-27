/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.util;


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
    E put(E element, boolean isTransactional);
   
    /**
     * 
     * @param key
     * @param isTransactional
     * @return 
     */
    E remove(K key, boolean isTransactional);
}
