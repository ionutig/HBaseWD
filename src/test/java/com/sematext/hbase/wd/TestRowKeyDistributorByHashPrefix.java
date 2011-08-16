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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Alex Baranau
 */
public class TestRowKeyDistributorByHashPrefix extends HBaseWdTestUtil {
  @Test
  public void testSimpleScanWithOneByteSimpleHash() throws IOException, InterruptedException {
    AbstractRowKeyDistributor keyDistributor =
            new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(15));
    testSimpleScan(keyDistributor);

    // Testing simple get
    byte[] originalKey = new byte[] {123, 124, 122};
    Put put = new Put(keyDistributor.getDistributedKey(originalKey));
    put.add(CF, QUAL, Bytes.toBytes("some"));
    hTable.put(put);

    byte[] distributedKey = keyDistributor.getDistributedKey(originalKey);
    Result result = hTable.get(new Get(distributedKey));
    Assert.assertArrayEquals(originalKey, keyDistributor.getOriginalKey(result.getRow()));
    Assert.assertArrayEquals(Bytes.toBytes("some"), result.getValue(CF, QUAL));
  }

  @Test
  public void testMapreduceJobWithOneByteSimpleHash() throws IOException, ClassNotFoundException, InterruptedException {
    AbstractRowKeyDistributor keyDistributor =
            new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(17));
    testMapReduce(keyDistributor);
  }

  public static class EmptyPrefixHasher implements RowKeyDistributorByHashPrefix.Hasher {
    private static final byte[] EMPTY_PREFIX = new byte[0];

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      return EMPTY_PREFIX;
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return new byte[][] {EMPTY_PREFIX};
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      // the original key wasn't changed
      return 0;
    }

    @Override
    public String getParamsToStore() {
      return null;
    }

    @Override
    public void init(String storedParams) {
      // DO NOTHING
    }
  }

  @Test
  public void testSimpleScanWithIdentityHasher() throws IOException, InterruptedException {
    AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(new EmptyPrefixHasher());
    testSimpleScan(keyDistributor);

    // Testing simple get
    byte[] originalKey = new byte[] {123, 124, 122};
    // No need to adjust key here as hasher doesn't change it
    Put put = new Put(originalKey);
    put.add(CF, QUAL, Bytes.toBytes("some"));
    hTable.put(put);

    byte[] distributedKey = keyDistributor.getDistributedKey(originalKey);
    Result result = hTable.get(new Get(distributedKey));
    Assert.assertArrayEquals(originalKey, keyDistributor.getOriginalKey(result.getRow()));
    Assert.assertArrayEquals(Bytes.toBytes("some"), result.getValue(CF, QUAL));
  }

  @Test
  public void testMapreduceJobWithIdentityHasher() throws IOException, ClassNotFoundException, InterruptedException {
    AbstractRowKeyDistributor keyDistributor =
            new RowKeyDistributorByHashPrefix(new EmptyPrefixHasher());
    testMapReduce(keyDistributor);
  }


  public static class MultiBytesPrefixHasher implements RowKeyDistributorByHashPrefix.Hasher {
    private static final byte[] PREFIX = new byte[] {(byte) 23,(byte) 55};

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      return PREFIX;
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return new byte[][] {PREFIX};
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      // the original key wasn't changed
      return PREFIX.length;
    }

    @Override
    public String getParamsToStore() {
      return null;
    }

    @Override
    public void init(String storedParams) {
      // DO NOTHING
    }
  }

  @Test
  public void testSimpleScanWithMultiBytesPrefixHasher() throws IOException, InterruptedException {
    AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(new MultiBytesPrefixHasher());
    testSimpleScan(keyDistributor);

    // Testing simple get
    byte[] originalKey = new byte[] {123, 124, 122};
    // No need to adjust key here as hasher doesn't change it
    Put put = new Put(keyDistributor.getDistributedKey(originalKey));
    put.add(CF, QUAL, Bytes.toBytes("some"));
    hTable.put(put);

    byte[] distributedKey = keyDistributor.getDistributedKey(originalKey);
    Result result = hTable.get(new Get(distributedKey));
    Assert.assertArrayEquals(originalKey, keyDistributor.getOriginalKey(result.getRow()));
    Assert.assertArrayEquals(Bytes.toBytes("some"), result.getValue(CF, QUAL));
  }

  @Test
  public void testMapreduceJobWithMultiBytesPrefixHasher() throws IOException, ClassNotFoundException, InterruptedException {
    AbstractRowKeyDistributor keyDistributor =
            new RowKeyDistributorByHashPrefix(new MultiBytesPrefixHasher());
    testMapReduce(keyDistributor);
  }
}
