
package se.sics.clusterj;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Index;

/**
 *
 CREATE TABLE `DatanodeInfo` ( 
	`storageId`	varchar(128) NOT NULL,
	`hostname` 	varchar(25) NOT NULL,
	`localPort`	int(11) NOT NULL,
	`infoPort` 	int(11) NOT NULL,
	`ipcPort`  	int(11) NOT NULL,
	`status`   	int(11) NULL DEFAULT '0',
	`location` 	varchar(25) NULL 
	)
  PRIMARY KEY (`storageId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1

 */
@PersistenceCapable(table="DatanodeInfo")
public interface DatanodeInfoTable {

    @PrimaryKey
    @Column(name = "storageId")
    String getStorageId();     
    void setStorageId(String storageId);
    
    @Column(name = "hostname")
    String getHostname();
    void setHostname(String hostname);
      
    @Column(name = "localPort")
    int getLocalPort();
    void setLocalPort(int localPort);
    
    @Column(name = "infoPort")
    int getInfoPort();
    void setInfoPort(int infoPort);

    @Column(name = "ipcPort")
    int getIpcPort();
    void setIpcPort(int ipcPort);

    /*
     0 --> NORMAL("In Service"), 
     1-->  DECOMMISSION_INPROGRESS("Decommission In Progress"), 
     2 --> DECOMMISSIONED("Decommissioned");
     */
    @Column(name = "status")
    int getStatus();
    void setStatus(int status);

    @Column(name = "location")
    String getLocation();
    void setLocation(String location);
    
}
