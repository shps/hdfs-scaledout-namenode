/**
 * 
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Index;


/** Optimized version of INodeTable which does not store the full path name of a file
 * 
 * @author wmalik
 */

@PersistenceCapable(table="INodeTableSimple")
public interface INodeTableSimple {

    @PrimaryKey
    @Column(name = "id")
    long getId ();     // id of the inode
    void setId (long id);

    @Column(name = "name")
    @Index(name="idx_name_parentid")
    String getName ();     //name of the inode
    void setName (String name);
    
    //id of the parent inode 
    @Column(name = "parentid")
    @Index(name="idx_name_parentid")
    long getParentID ();     // id of the inode
    void setParentID (long parentid);
    
    // marker for InodeDirectory
    @Column(name = "isDir")
    boolean getIsDir ();
    void setIsDir (boolean isDir);

    // marker for InodeDirectoryWithQuota
    @Column(name = "isDirWithQuota")
    boolean getIsDirWithQuota ();
    void setIsDirWithQuota (boolean isDirWithQuota);

    // Inode
    @Column(name = "modificationTime")
    long getModificationTime ();
    void setModificationTime (long modificationTime);

    // Inode
    @Column(name = "aTime")
    long getATime ();
    void setATime (long modificationTime);

    // Inode
    
//    @Lob
    @Column(name = "permission")
    byte[] getPermission (); 
    void setPermission (byte[] permission);

    // InodeDirectoryWithQuota
    @Column(name = "nscount")
    long getNSCount ();
    void setNSCount (long nsCount);

    // InodeDirectoryWithQuota
    @Column(name = "dscount")
    long getDSCount ();
    void setDSCount (long dsCount);

    // InodeDirectoryWithQuota
    @Column(name = "nsquota")
    long getNSQuota ();
    void setNSQuota (long nsQuota);

    // InodeDirectoryWithQuota
    @Column(name = "dsquota")
    long getDSQuota ();
    void setDSQuota (long dsQuota);

    //  marker for InodeFileUnderConstruction
    @Column(name = "isUnderConstruction")
    boolean getIsUnderConstruction ();
    void setIsUnderConstruction (boolean isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = "clientName")
    String getClientName ();
    void setClientName (String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = "clientMachine")
    String getClientMachine ();
    void setClientMachine (String clientMachine);

    // InodeFileUnderConstruction -- TODO
    @Column(name = "clientNode")
    String getClientNode ();
    void setClientNode (String clientNode);

    //  marker for InodeFile
    @Column(name = "isClosedFile")
    boolean getIsClosedFile ();
    void setIsClosedFile (boolean isClosedFile);

    // InodeFile
    @Column(name = "header")
    long getHeader ();
    void setHeader (long header);

    
    //INodeSymlink
    @Column (name = "symlink")
    byte[] getSymlink ();
    void setSymlink(byte[] symlink);
   
}
