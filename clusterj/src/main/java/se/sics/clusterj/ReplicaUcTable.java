/**
 * 
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;

/**
 * @author wmalik
 *
 */
@PersistenceCapable(table="ReplicaUc")
public interface ReplicaUcTable {

    @Column(name = "blockId")
    long getBlockId();     
    void setBlockId(long blkid);
    
    @Column(name = "expLocation")
    byte[] getExpectedLocation();  
    void setExpectedLocation(byte[] expiryDate);
    
    @Column(name = "state")
    byte[] getState(); 
    void setState(byte[] state);
    
    @Column(name = "timestamp")
    long getTimestamp();     
    void setTimestamp(long ts);
    
}

