package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 *
 * @author jude
 */
@PersistenceCapable(table="UnderReplicaBlocks")
public interface UnderReplicaBlocksTable 
{
    @PrimaryKey
    @Column(name = "blockId")
    long getBlockId();
    void setBlockId(long bid);

    @Column(name = "level")
    int getLevel();
    void setLevel(int level);
}
