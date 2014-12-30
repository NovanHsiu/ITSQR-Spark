/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.SparseRowBlock
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.stsqr.ssvd;

import java.util.Arrays;
import java.io.Serializable;

import no.uib.cipr.matrix.Vector;

/**
 * Aggregate incoming rows into blocks based on the row number (long). Rows can
 * be sparse (meaning they come perhaps in big intervals) and don't even have to
 * come in any order, but they should be coming in proximity, so when we output
 * block key, we hopefully aggregate more than one row by then.
 * <P>
 * 
 * If block is sufficiently large to fit all rows that mapper may produce, it
 * will not even ever hit a spill at all as we would already be plussing
 * efficiently in the mapper.
 * <P>
 * 
 * Also, for sparse inputs it will also be working especially well if transposed
 * columns of the left side matrix and corresponding rows of the right side
 * matrix experience sparsity in same elements.
 * <P>
 */

public class SparseRowBlock implements Serializable{

	private int[] rowIndices;
	private Vector[] rows;
	private int numRows;
	
	public SparseRowBlock() {
    this(10);
	}

	public SparseRowBlock(int initialCapacity) {
    rowIndices = new int[initialCapacity];
    rows = new Vector[initialCapacity];
	}
  
    public int[] getRowIndices() {
    return rowIndices;
	}

	public Vector[] getRows() {
    return rows;
	}
	
	public void plusRow(int index, Vector row) {
    /*
     * often accumulation goes in row-increasing order, so check for this to
     * avoid binary search (another log Height multiplier).
     */
    int pos =
      numRows == 0 || rowIndices[numRows - 1] < index ? -numRows - 1 : Arrays
        .binarySearch(rowIndices, 0, numRows, index);
	//puls "row" and "rows[pos]" vector
    if (pos >= 0) {
	  rows[pos] = rows[pos].add(row);
    } else {
      insertIntoPos(-pos - 1, index, row);
    }
	}

	private void insertIntoPos(int pos, int rowIndex, Vector row) {
    // reallocate if needed
    if (numRows == rows.length) {
      rows = Arrays.copyOf(rows, numRows + 1 << 1);
      rowIndices = Arrays.copyOf(rowIndices, numRows + 1 << 1);
    }
    // make a hole if needed
    System.arraycopy(rows, pos, rows, pos + 1, numRows - pos);
    System.arraycopy(rowIndices, pos, rowIndices, pos + 1, numRows - pos);
    // put
    rowIndices[pos] = rowIndex;
    rows[pos] = row;
    numRows++;
	}

  /**
   * pluses one block into another. Use it for accumulation of partial products in
   * combiners and reducers.
   * 
   * @param bOther
   *          block to add
   */
	public void plusBlock(SparseRowBlock bOther) {
    /*
     * since we maintained row indices in a sorted order, we can run sort merge
     * to expedite this operation
     */
    int i = 0;
    int j = 0;
    while (i < numRows && j < bOther.numRows) {
      while (i < numRows && rowIndices[i] < bOther.rowIndices[j]) {
        i++;
      }
      if (i < numRows) {
        if (rowIndices[i] == bOther.rowIndices[j]) {
		  rows[i] = rows[i].add(bOther.rows[j]);
        } else {
          // insert into i-th position
          insertIntoPos(i, bOther.rowIndices[j], bOther.rows[j]);
        }
        // increment in either case
        i++;
        j++;
      }
    }
    for (; j < bOther.numRows; j++) {
      insertIntoPos(numRows, bOther.rowIndices[j], bOther.rows[j]);	  
    }
	}

	public int getNumRows() {
    return numRows;
	}
  
}