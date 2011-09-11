package com.sematext.hbase.wd;

public class RowKeyDistributorByHashPrefix_MultiBytesPrefixHashTest extends HBaseWdTestBase {
  public RowKeyDistributorByHashPrefix_MultiBytesPrefixHashTest() {
    super(new RowKeyDistributorByHashPrefix(new MultiBytesPrefixHash()));
  }

  public static class MultiBytesPrefixHash implements RowKeyDistributorByHashPrefix.Hasher {
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
}