package org.apache.hadoop.hdfs.server.namenode;

/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;

/**
 *
 * @author kamal
 */
public class TestReadingWritingNN extends TestCase {

    public TestReadingWritingNN() {
    }

    public void testWritingAndReading() throws Exception {
        Configuration conf = new HdfsConfiguration();
        conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numRNameNodes(2).numDataNodes(2).build();
        try {
                    FsUrlStreamHandlerFactory factory =
                new org.apache.hadoop.fs.FsUrlStreamHandlerFactory();
        java.net.URL.setURLStreamHandlerFactory(factory);

            FileSystem wfs = cluster.getWritingFileSystem();
            //writing two block file
            final long LONG_FILE_LEN = 128 * 1024 * 1024;
            final Path filePath = new Path("/tmp.txt");
            DFSTestUtil.createFile(wfs, filePath,
                    LONG_FILE_LEN, (short) 1, 1L);
            
            //read the file from first reading NN
            FileSystem rfs1 = cluster.getReadingFileSystem(0, 0);
            URI uri = rfs1.getUri();
            
            URL fileURL = new URL(uri.getScheme(), uri.getHost(), 
                    uri.getPort(), filePath.toString());
            
            InputStream is = fileURL.openStream();
            assertNotNull(is);

            byte[] bytes = new byte[4096];
            assertEquals(4096, is.read(bytes));
            is.close();

            //read the file from second reading NN
            FileSystem rfs2 = cluster.getReadingFileSystem(0, 0);
            URI uri2 = rfs2.getUri();
            
            URL fileURL2 = new URL(uri2.getScheme(), uri2.getHost(), 
                    uri2.getPort(), filePath.toString());
            
            InputStream is2 = fileURL2.openStream();
            assertNotNull(is2);

            byte[] bytes2 = new byte[4096];
            assertEquals(4096, is2.read(bytes));
            is2.close();
            
            // delete the file
            wfs.delete(filePath, false);

        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }
}
