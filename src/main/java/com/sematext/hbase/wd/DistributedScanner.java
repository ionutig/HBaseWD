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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface for client-side scanning the data written with keys distribution
 *
 * @author Alex Baranau
 */
public class DistributedScanner implements ResultScanner {
  private final ResultScanner[] scanners;
  private final List<Result>[] nextOfScanners;
  private Result next = null;

  @SuppressWarnings("unchecked")
  public DistributedScanner(ResultScanner[] scanners) throws IOException {
    this.scanners = scanners;
    this.nextOfScanners = new List[scanners.length];
    Arrays.fill(this.nextOfScanners, new ArrayList<Result>());
  }

  private boolean hasNext(int nbRows) throws IOException {
    if (next != null) {
      return true;
    }

    next = nextInternal(nbRows);

    return next != null;
  }

  @Override
  public Result next() throws IOException {
    if (hasNext(1)) {
      Result toReturn = next;
      next = null;
      return toReturn;
    }

    return null;
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    // Identical to HTable.ClientScanner implementation
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }

  @Override
  public void close() {
    for (int i = 0; i < scanners.length; i++) {
      scanners[i].close();
    }
  }

  public static DistributedScanner create(HTable hTable, Scan original, AbstractRowKeyDistributor keyDistributor) throws IOException {
    byte[][] startKeys = keyDistributor.getAllDistributedKeys(original.getStartRow());
    byte[][] stopKeys = keyDistributor.getAllDistributedKeys(original.getStopRow());
    Scan[] scans = new Scan[startKeys.length];
    for (byte i = 0; i < startKeys.length; i++) {
      scans[i] = new Scan(original);
      scans[i].setStartRow(startKeys[i]);
      scans[i].setStopRow(stopKeys[i]);
    }

    ResultScanner[] rss = new ResultScanner[startKeys.length];
    for (byte i = 0; i < scans.length; i++) {
      rss[i] = hTable.getScanner(scans[i]);
    }

    return new DistributedScanner(rss);
  }

  private Result nextInternal(int nbRows) throws IOException {
    Result result = null;
    byte indexOfScannerToUse = -1;
    for (byte i = 0; i < nextOfScanners.length; i++) {
      if (nextOfScanners[i] == null) {
        // result scanner is exhausted, don't advance it any more
        continue;
      }

      if (nextOfScanners[i].size() == 0) {
        // advancing result scanner
        Result[] results = scanners[i].next(nbRows);
        if (results.length == 0) {
          // marking result scanner as exhausted
          nextOfScanners[i] = null;
          continue;
        }
        nextOfScanners[i].addAll(Arrays.asList(results));
      }

      if (result == null || Bytes.compareTo(nextOfScanners[i].get(0).getRow(), result.getRow()) < 0) {
        result = nextOfScanners[i].get(0);
        indexOfScannerToUse = i;
      }
    }

    if (indexOfScannerToUse >= 0) {
      nextOfScanners[indexOfScannerToUse].remove(0);
    }

    return result;
  }

  @Override
  public Iterator<Result> iterator() {
    // Identical to HTable.ClientScanner implementation
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      public boolean hasNext() {
        if (next == null) {
          try {
            next = DistributedScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
