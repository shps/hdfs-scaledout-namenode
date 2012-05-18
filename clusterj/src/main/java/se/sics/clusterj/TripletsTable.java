/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Index;

/**
 *
 * @author wmalik
 */
@PersistenceCapable(table="triplets")
public interface TripletsTable {

    @PrimaryKey
    @Column(name = "blockId")
    long getBlockId();     
    void setBlockId(long bid);
    
    @PrimaryKey
    @Column(name = "index")
    int getIndex();
    void setIndex(int index);
    
      
    @Column(name = "storageId")
    @Index(name="idx_datanodeStorage")
    String getStorageId();
    void setStorageId(String id);
}
