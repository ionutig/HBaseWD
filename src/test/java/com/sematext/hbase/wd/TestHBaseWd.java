/**
 * Copyright 2010 Sematext International
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sematext.hbase.wd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * General unit-test for the whole concept 
 *
 * @author Alex Baranau
 */
public class TestHBaseWd {
  private HBaseTestingUtility testingUtility;
  public static final byte[] CF = Bytes.toBytes("colfam");
  public static final byte[] QUAL = Bytes.toBytes("qual");

  @Before
  public void before() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster(1);
  }

  @After
  public void after() throws IOException {
    testingUtility.shutdownMiniCluster();
    testingUtility = null;
  }

  @Test
  public void testSimpleScan() throws IOException, InterruptedException {
    byte bucketsCount = (byte) 12;

    HTable hTable = testingUtility.createTable(Bytes.toBytes("table"), CF);
    long millis = System.currentTimeMillis();
    int valuesCountInSeekInterval = 0;

    AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByOneBytePrefix(bucketsCount);
    for (int i = 0; i < 500; i++) {
      int val = 500 + i - i * (i % 2) * 2; // i.e. 500, 499, 502, 497, 504, ...
      valuesCountInSeekInterval += (val >= 100 && val < 900) ? 1 : 0;
      byte[] key = Bytes.toBytes(millis + val);
      Put put = new Put(keyDistributor.getDistributedKey(key));
      put.add(CF, QUAL, Bytes.toBytes(val));
      hTable.put(put);
    }

    byte[] startKey = Bytes.toBytes(millis + 100);
    byte[] stopKey = Bytes.toBytes(millis + 900);

    // TODO: add some filters for better testing
    Scan scan = new Scan(startKey, stopKey);
    ResultScanner distributedScanner = DistributedScanner.create(hTable, scan, keyDistributor);

    Result previous = null;
    int countMatched = 0;
    for (Result current : distributedScanner) {
      countMatched++;
      if (previous != null) {
        Assert.assertTrue(Bytes.compareTo(current.getRow(), previous.getRow()) >= 0);
        Assert.assertTrue(Bytes.toInt(current.getValue(CF, QUAL)) >= 100);
        Assert.assertTrue(Bytes.toInt(current.getValue(CF, QUAL)) < 900);
      }
      previous = current;
    }

    Assert.assertEquals(valuesCountInSeekInterval, countMatched);
  }

  @Test
  public void testMapreduceJob() throws IOException, ClassNotFoundException, InterruptedException {
    byte spread = (byte) 12;

    // Writing data
    AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByOneBytePrefix(spread);
    HTable hTable = testingUtility.createTable(Bytes.toBytes("table"), CF);
    long millis = System.currentTimeMillis();
    int valuesCountInSeekInterval = 0;
    for (int i = 0; i < 500; i++) {
      int val = 500 + i - i * (i % 2) * 2; // i.e. 500, 499, 502, 497, 504, ...
      valuesCountInSeekInterval += (val >= 100 && val < 900) ? 1 : 0;
      byte[] key = Bytes.toBytes(millis + val);
      Put put = new Put(keyDistributor.getDistributedKey(key));
      put.add(CF, QUAL, Bytes.toBytes(val));
      hTable.put(put);
    }

    // Reading data
    byte[] startKey = Bytes.toBytes(millis + 100);
    byte[] stopKey = Bytes.toBytes(millis + 900);
    Scan scan = new Scan(startKey, stopKey);

    Configuration conf = HBaseConfiguration.create();

    Job job = new Job(conf, "testMapreduceJob");
    TableMapReduceUtil.initTableMapperJob("table", scan,
      RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

    // Substituting standard TableInputFormat which was set in TableMapReduceUtil.initTableMapperJob(...)
    job.setInputFormatClass(WdTableInputFormat.class);
    keyDistributor.addInfo(job.getConfiguration());

    job.setJarByClass(TestHBaseWd.class);

    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);

    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);

    long mapInputRecords = job.getCounters().findCounter(RowCounterMapper.Counters.ROWS).getValue();
    Assert.assertEquals(valuesCountInSeekInterval, mapInputRecords);
  }

  /**
   * Mapper that runs the count.
   * NOTE: it was copied from RowCounter class
   */
  static class RowCounterMapper
  extends TableMapper<ImmutableBytesWritable, Result> {
    /** Counter enumeration to count the actual rows. */
    public static enum Counters {ROWS}

    @Override
    public void map(ImmutableBytesWritable row, Result values,
      Context context)
    throws IOException {
      for (KeyValue value: values.list()) {
        if (value.getValue().length > 0) {
          context.getCounter(Counters.ROWS).increment(1);
          break;
        }
      }
    }
  }
}
