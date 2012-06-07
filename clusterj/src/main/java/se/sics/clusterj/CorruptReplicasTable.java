/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 *
 * @author jude
 */
@PersistenceCapable(table="CorruptReplicas")
public interface CorruptReplicasTable {
    @PrimaryKey
    @Column(name = "blockId")
    long getBlockId();
    void setBlockId(long bid);

    @PrimaryKey
    @Index(name="idx_datanodeStorage")
    String getStorageId();
    void setStorageId(String id);
}
