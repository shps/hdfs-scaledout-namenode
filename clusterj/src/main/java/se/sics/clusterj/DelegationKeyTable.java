/**
 * 
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 * @author wmalik
 *
 */
@PersistenceCapable(table="DelegationKey")
public interface DelegationKeyTable {

    @PrimaryKey
    @Column(name = "keyId")
    int getKeyId();     
    void setKeyId(int keyId);
    
    @Column(name = "expiryDate")
    long getExpiryDate();  
    void setExpiryDate(long expiryDate);
    
    @Column(name = "keyBytes")
    byte[] getKeyBytes(); 
    void setKeyBytes (byte[] keyBytes);
    
    
    /*CURR_KEY=0 NEXT_KEY=1*/
    @Column(name = "keyType")
    short getKeyType();  
    void setKeyType(short keyType);
}

