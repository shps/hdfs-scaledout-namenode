package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.log4j.NDC;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class LightWeightRequestHandler extends RequestHandler {

  public LightWeightRequestHandler(OperationType opType) {
    super(opType);
  }

  @Override
  protected Object run(boolean writeLock, boolean readLock, Namesystem namesystem) throws IOException {
    boolean systemLevelLock = FSNamesystem.systemLevelLock();
    if (systemLevelLock) {
      if (writeLock) {
        namesystem.writeLock();
      }
      if (readLock) {
        namesystem.readLock();
      }
    }
    boolean retry = true;
    int tryCount = 0;
    IOException exception = null;

    try {
      while (retry && tryCount < RETRY_COUNT) {
        retry = true;
        tryCount++;
        exception = null;
        try {
          NDC.push(opType.name()); // Defines a context for every operation to track them in the logs easily.
          return performTask();
        } catch (PersistanceException ex) {
          log.error("Could not perfortm task", ex);
          retry = true;
        } catch (IOException ex) {
          exception = ex;
        } finally {
          NDC.pop();
          if (tryCount == RETRY_COUNT && exception != null) {
            throw exception;
          }
        }
      }
    } finally {
      if (systemLevelLock) {
        if (writeLock) {
          namesystem.writeUnlock();
        }
        if (readLock) {
          namesystem.readUnlock();
        }
      }
    }
    return null;
  }
}
