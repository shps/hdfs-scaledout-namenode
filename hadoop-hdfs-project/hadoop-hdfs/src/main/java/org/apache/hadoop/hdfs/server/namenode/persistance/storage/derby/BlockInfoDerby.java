package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoDerby extends BlockInfoStorage {

  private DerbyConnector connector = DerbyConnector.INSTANCE;
  protected Map<Long, BlockInfo> newBlocks = new HashMap<Long, BlockInfo>();

  @Override
  public void remove(BlockInfo block) throws TransactionContextException {
    if (block.getBlockId() == 0l) {
      throw new TransactionContextException("Unassigned-Id block passed to be removed");
    }

    BlockInfo attachedBlock = blocks.get(block.getBlockId());

    if (attachedBlock == null) {
      throw new TransactionContextException("Unattached block passed to be removed");
    }

    blocks.remove(block.getBlockId());
    modifiedBlocks.remove(block.getBlockId());
    newBlocks.remove(block.getBlockId());
    removedBlocks.put(block.getBlockId(), attachedBlock);
  }

  @Override
  public Collection<BlockInfo> findList(Finder<BlockInfo> finder, Object... params) {
    BlockInfoFinder bFinder = (BlockInfoFinder) finder;
    List<BlockInfo> result = null;
    switch (bFinder) {
      case ByInodeId:
        long inodeId = (Long) params[0];
        result = findByInodeId(inodeId);
        break;
      case ByStorageId:
        String storageId = (String) params[0];
        result = findByStorageId(storageId);
        break;
      case All:
        result = findAllBlocks();
    }

    return result;
  }

  @Override
  public BlockInfo find(Finder<BlockInfo> finder, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(BlockInfo block) throws TransactionContextException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    modifiedBlocks.put(block.getBlockId(), block);
  }

  @Override
  public void add(BlockInfo block) throws TransactionContextException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    newBlocks.put(block.getBlockId(), block);
  }

  @Override
  public void commit() {
    String insertQuery = "insert into block_info values(?,?,?,?,?,?,?,?,?)";
    String deleteQuery = "delete from block_info where blockId = ?";
    String updateQuery = "update block_info set "
            + "blockIndex=?, iNodeID=?, numBytes=?, generationStamp=?,"
            + "BlockUCState=?, \"timestamp=\"=?, primaryNodeIndex=?,"
            + "blockRecoveryId=? where blockId=?";
    try {
      Connection conn = connector.obtainSession();

      PreparedStatement dlt = conn.prepareStatement(deleteQuery);
      for (BlockInfo block : removedBlocks.values()) {
        dlt.setLong(1, block.getBlockId());
        dlt.addBatch();
      }

      PreparedStatement insrt = conn.prepareStatement(insertQuery);
      for (BlockInfo block : newBlocks.values()) {
        insrt.setLong(1, block.getBlockId());
        insrt.setInt(2, block.getBlockIndex());
        insrt.setLong(3, block.getINode().getID());
        insrt.setLong(4, block.getNumBytes());
        insrt.setLong(5, block.getGenerationStamp());
        insrt.setInt(6, block.getBlockUCState().ordinal());
        insrt.setLong(7, block.getTimestamp());
        if (block instanceof BlockInfoUnderConstruction) {
          BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
          insrt.setInt(8, ucBlock.getPrimaryNodeIndex());
          insrt.setLong(9, ucBlock.getBlockRecoveryId());
        }
        insrt.addBatch();
      }

      PreparedStatement updt = conn.prepareStatement(updateQuery);
      for (BlockInfo block : modifiedBlocks.values()) {
        updt.setLong(9, block.getBlockId());
        updt.setInt(1, block.getBlockIndex());
        updt.setLong(2, block.getINode().getID());
        updt.setLong(3, block.getNumBytes());
        updt.setLong(4, block.getGenerationStamp());
        updt.setInt(5, block.getBlockUCState().ordinal());
        updt.setLong(6, block.getTimestamp());
        if (block instanceof BlockInfoUnderConstruction) {
          BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
          updt.setInt(7, ucBlock.getPrimaryNodeIndex());
          updt.setLong(8, ucBlock.getBlockRecoveryId());
        }
        updt.addBatch();
      }

      dlt.executeBatch();
      insrt.executeBatch();
      updt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private List<BlockInfo> findByInodeId(long id) {
    if (inodeBlocks.containsKey(id)) {
      return inodeBlocks.get(id);
    } else {

      List<BlockInfo> syncedList = null;
      try {
        Connection conn = connector.obtainSession();
        String query = "select * from block_info where iNodeID = ?";
        PreparedStatement s;
        s = conn.prepareStatement(query);

        ResultSet result = s.executeQuery();
        syncedList = syncBlockInfoInstances(BlockInfoFactory.createBlockInfoList(resultList));
        inodeBlocks.put(id, syncedList);
      } catch (SQLException ex) {
        Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
      }

      return syncedList;
    }
  }
}
