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
// modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.SparseRowBlockAccumulator
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.stsqr.ssvd;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import scala.Tuple2;

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
 *-----
 * part of Modification:
 * 1. Replaced mahout Vector by mtj Vector.
 */

public class SparseRowBlockAccumulator{

	private final int height;
	private int currentBlockNum = -1;
	private List<Tuple2<Integer,SparseRowBlock>> blockList;
	private List<Integer> existBlockList;
	
	public SparseRowBlockAccumulator(int height)
	{
	 this.height = height;
	 blockList = new ArrayList<Tuple2<Integer,SparseRowBlock>>();
	 existBlockList = new ArrayList<Integer>();
	}
	
	private void addBlock(int blockKey, SparseRowBlock block) throws IOException {
	 //store the block in List
	 existBlockList.add(blockKey);
	 blockList.add(new Tuple2<Integer,SparseRowBlock>(blockKey,block));
   }

  public void collect(int rowIndex, Vector v) throws IOException { 
	  int blockKey = rowIndex / height;    
	  int eindex = existBlockList.indexOf(blockKey);
	  if(eindex!=-1) //exist
	  {
	   blockList.get(eindex)._2.plusRow((int) (rowIndex % height), v);
	  }
	  else //block not exist in the list
	  {
	   SparseRowBlock block = new SparseRowBlock(100);
	   block.plusRow((int) (rowIndex % height), v);
       addBlock(blockKey,block);
	  }
	 
	/*if (blockKey != currentBlockNum) {
    }*/
   
  }
  
  public List<Tuple2<Integer,SparseRowBlock>> getBlockList() throws IOException {
	return blockList;
  }
  
}