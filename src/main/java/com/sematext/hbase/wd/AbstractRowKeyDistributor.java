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

import org.apache.hadoop.conf.Configuration;

/**
 * Defines the way row keys are distributed
 *
 * @author Alex Baranau
 */
public abstract class AbstractRowKeyDistributor implements Parametrizable {
  public abstract byte[] getDistributedKey(byte[] originalKey);

  public abstract byte[] getOriginalKey(byte[] adjustedKey);

  public abstract byte[][] getAllDistributedKeys(byte[] originalKey);

  public void addInfo(Configuration conf) {
    conf.set(WdTableInputFormat.ROW_KEY_DISTRIBUTOR_CLASS, this.getClass().getCanonicalName());
    conf.set(WdTableInputFormat.ROW_KEY_DISTRIBUTOR_PARAMS, getParamsToStore());
  }
}
