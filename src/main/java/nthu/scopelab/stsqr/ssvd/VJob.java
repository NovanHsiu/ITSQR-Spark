/**
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
// modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.VJob
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.stsqr.ssvd;

import java.lang.Math;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.matrix.VectorWritable;

import java.io.IOException;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.DenseVector;
/**
 * Computes V=Bt*Uhat*Sigma^(-1) of SSVD
 **/
public class VJob{

  private JavaPairRDD<IntWritable,VectorWritable> Btrdd;
  private JavaPairRDD<LongWritable,sLMatrixWritable> Vrdd;
  private int k, rbs;
  private cmDenseMatrix Uhat;
  private Vector sValues;
  
  public VJob(JavaPairRDD<IntWritable,VectorWritable> Btrdd,cmDenseMatrix Uhat,Vector sValues,int k,int p,int rbs,boolean vHalfSigma)
  {
	this.Btrdd = Btrdd;
	this.Uhat = Uhat;
	this.sValues = sValues;
	this.k = k;
	this.rbs = rbs;
	
	if (vHalfSigma) {
        for (int i = 0; i < k; i++) {
          sValues.set(i, Math.sqrt(sValues.get(i)));
        }
    }
	
  }
  
  public JavaPairRDD<LongWritable,sLMatrixWritable> getVrdd()
  {
   return Vrdd;
  }
  
  public void run() throws IOException
  {
   Vrdd = Btrdd.map(new VMapFunction(Uhat,sValues,k)).groupByKey().flatMap(new Vmerge(rbs));
  }
  
  protected static class VMapFunction extends PairFunction<Tuple2<IntWritable,VectorWritable>,Integer,Tuple2<Integer,VectorWritable>> {
	
	private cmDenseMatrix uHat;
	private Vector sValues;
	private int k;
	private int leaderkey = -1;
	
	public VMapFunction(cmDenseMatrix uHat,Vector sValues,int k)
	{
	 this.uHat = uHat;
	 this.sValues = sValues;
	 this.k = k;
	}
	
	public Tuple2<Integer,Tuple2<Integer,VectorWritable>> call(Tuple2<IntWritable,VectorWritable> kvpair) throws IOException {
	  
	  if(leaderkey==-1)
	   leaderkey = kvpair._1.get();
	  Vector btRow = kvpair._2.get();
	  Vector vRow = new DenseVector(k);
      for (int i = 0; i < k; i++) {
        vRow.set(i,btRow.dot(new DenseVector(uHat.getColumn(i))) / sValues.get(i));
      }
	  return new Tuple2<Integer,Tuple2<Integer,VectorWritable>>(leaderkey,new Tuple2<Integer,VectorWritable>(kvpair._1.get(),new VectorWritable((DenseVector)vRow)));
    }
	 
  }
  
 private static class Vmerge extends PairFlatMapFunction<Tuple2< Integer, List<Tuple2<Integer,VectorWritable>> >, LongWritable, sLMatrixWritable>
 {
  private int cols = -1, rbs;
  
  public Vmerge(int rbs)
  {
   this.rbs = rbs;
  }
  
  public Iterable<Tuple2<LongWritable, sLMatrixWritable>> call(Tuple2< Integer, List<Tuple2<Integer,VectorWritable>> > kvpair) {
   
   List<Tuple2<Integer,VectorWritable>> rowslist = kvpair._2;
   cols = rowslist.get(0)._2.get().size();
   int remainRowSize = rowslist.size()%rbs;
   int numSplit = rowslist.size()/rbs;
   if(remainRowSize>0)
    numSplit+=1;
	
   int finalRowSize = remainRowSize;
   if(remainRowSize<cols && remainRowSize!=0)
   {
    if(numSplit==0)
	{
	 //throw new Exception(" Exception in 'mergeRows' function! Have too many tasks and input size of matrix row is too small! Please reduce the number of tasks of previous Rdd which use 'makeRows' function. ");
	}
    //merge the last submatrix to second last submatrix
    finalRowSize =+rbs;
	numSplit-=1;
   }
   List<Tuple2<LongWritable, sLMatrixWritable>> outputList = new ArrayList<Tuple2<LongWritable, sLMatrixWritable>>();
   int rows = rbs;
   int count = 0;
   int itemindex;
   DenseVector vec;
   for(int i=0;i<numSplit;i++)
   {
	if(i==numSplit-1 && remainRowSize>0)
	 rows = finalRowSize;
	
    long[] larray = new long[rows];
	cmDenseMatrix mat = new cmDenseMatrix(rows,cols);
	for(int j=0;j<rows;j++)
	{ 
	 itemindex = rowslist.get(count)._1;
	 vec = (DenseVector)rowslist.get(count)._2.get();
	 larray[j] = (long) itemindex;
	 mat.setRow(j,vec.getData());	
	 count++;
	}

	outputList.add(new Tuple2<LongWritable,sLMatrixWritable>(new LongWritable(larray[0]),new sLMatrixWritable(larray,mat)));
   }
  
	return outputList;
   }
   
  /*
  //call method for PairFunction version
  public Tuple2<LongWritable, sLMatrixWritable> call(Tuple2< Integer, List<Tuple2<Integer,VectorWritable>> > kvpair) {
   
   List<Tuple2<Integer,VectorWritable>> valuelist = kvpair._2;
   int itemindex;
   DenseVector vec;
   int rows = valuelist.size(), cols = valuelist.get(0)._2.get().size();
   cmDenseMatrix mat = new cmDenseMatrix(rows,cols);
   long[] larray = new long[rows];
   
   for(int i=0;i<valuelist.size();i++)
   {
    itemindex = valuelist.get(i)._1;
	vec = (DenseVector)valuelist.get(i)._2.get();
	larray[i] = (long) itemindex;
	mat.setRow(i,vec.getData());
   }
	return new Tuple2<LongWritable,sLMatrixWritable>(new LongWritable(larray[0]),new sLMatrixWritable(larray,mat));
   }*/
  }

}
