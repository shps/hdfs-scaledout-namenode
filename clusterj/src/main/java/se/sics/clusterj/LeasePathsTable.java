package se.sics.clusterj;

import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.*;


/**
 * This is a ClusterJ interface for interacting with the "LeasePaths" table
 * @author wmalik
 */
@PersistenceCapable(table="LeasePath")
public interface LeasePathsTable {
	
	@Column(name = "holderID")
	int getHolderID();
	void setHolderID(int holder_id);
	
	@PrimaryKey
	@Column(name = "path")
	String getPath();
	void setPath(String path);
	
	
}
