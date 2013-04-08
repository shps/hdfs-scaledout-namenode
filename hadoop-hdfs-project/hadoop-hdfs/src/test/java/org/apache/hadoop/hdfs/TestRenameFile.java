package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class TestRenameFile {

    public static final Log LOG = LogFactory.getLog(TestRenameFile.class.getName());
    private final short DATANODES = 5;
    short replicas = 5;
    private int seed = 139;
    private int blockSize = 8192;
    Configuration conf;
    MiniDFSCluster cluster;
    DistributedFileSystem dfs;
    final int numBlocks = 2;
    final int fileSize = numBlocks * blockSize + 1;

    @Before
    public void initialize() throws IOException {
    }

    @After
    public void close() {
    }

    /**
     * Creates a file and writes a block using the returned output stream. It
     * checks if the block is written and checks if the number of located blocks
     * and replicas are as expected.
     *
     * @throws IOException
     */
    @Test
    public void testRenameFile() throws IOException {

        try {
            conf = new HdfsConfiguration();
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODES).build();
            cluster.waitActive();
            dfs = (DistributedFileSystem) cluster.getFileSystem();
            //DFSClient client = dfs.dfs;
            DFSClient client = dfs.getDefaultDFSClient();
            // create a new file.
            //
            Path file1 = new Path("/test/a");

            FSDataOutputStream strm = createFile(dfs, file1, replicas);


            // add one block to the file
            writeFile(strm, blockSize);
            strm.hflush();
            strm.close();
            
            
            
            if(!client.rename("/test/a", "/test/b"))
            {
                fail("rename file failed");
            }

            if (!client.delete("/test/b", true)) {
                fail("delete file failed");
            }
        } catch (Exception e) {
            //test fails if any thing goes worng
            LOG.equals(e);
            fail("");
        } finally {
            if (dfs != null) {
                IOUtils.closeStream(dfs);
            }
            cluster.shutdown();
            System.out.println("closed");
        }
    }

    
    //
    // writes specified bytes to file.
    //

    private void writeFile(FSDataOutputStream stm, int size) throws IOException {
        byte[] buffer = AppendTestUtil.randomBytes(seed, size);
        stm.write(buffer, 0, size);
    }

    // creates a file but does not close it
    private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
            throws IOException {
        LOG.info("createFile: Created " + name + " with " + repl + " replica.");
        FSDataOutputStream stm = fileSys.create(name, true,
                fileSys.getConf().getInt("io.file.buffer.size", 4000000),
                (short) repl, (long) blockSize);
        return stm;
    }

    void writeFile(FSDataOutputStream stm) throws IOException {
        writeFile(stm, fileSize);
    }
}
