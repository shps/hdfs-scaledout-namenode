package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.*;


/**
 * @author Jude
 *
 * This is a ClusterJ interface for interacting with the "BlockTotal" table
 *
 */
@PersistenceCapable(table="BlockTotal")
public interface BlockTotalTable {
	
        @PrimaryKey
        @Column(name = "id")
        int getId();
        void setId(int id);

       @Column(name = "Total")
        long getTotal();
        void setTotal(long total);
}
