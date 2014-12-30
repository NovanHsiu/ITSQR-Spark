package nthu.scopelab.stsqr.test;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.io.SequenceFile.Writer;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.math.QRF;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.matrix.sMatrixWritable;

import nthu.scopelab.stsqr.TSQR;
import nthu.scopelab.stsqr.QRJob;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Iterator;
import java.util.Date;

public class QRredScheJob extends TSQR {
  
  public static void main(String[] args) throws Exception {
	String inputpath = getArgument("-input",args);
	checkArgument(inputpath);
	String master = getArgument("-master",args);
	checkArgument(master);
	String outputpath = getArgument("-output",args);
	if(outputpath==null)
	 outputpath = "output";
	String redsche_str = getArgument("-reduceSchedule",args);
	if(redsche_str==null)
	 redsche_str = "1";
	String exememory = getArgument("-em",args);
	if(exememory==null)
	 exememory = "512m";
	
	String matsize_str = getArgument("-matsize",args);
	checkArgument(matsize_str);
	String matnum_str = getArgument("-matnum",args);
	checkArgument(matnum_str);
		
	String sparkHome = System.getenv("SPARK_HOME");
	SparkConf sconf = new SparkConf().setMaster(master)
	.setAppName("QRredScheJob")
	.setSparkHome(System.getenv("SPARK_HOME"))
	.setJars(new String[]{sparkHome+"/oproject/target/simple-project-1.0.jar"})
	.set("spark.executor.memory", exememory);
    JavaSparkContext ctx = new JavaSparkContext(sconf);
    // 1. prepartionJob: turn matrix from text(string) to hadoop sequencefile(sLMatrixWritable)
    Thread.sleep(2000);
	int matsize = Integer.valueOf(matsize_str);
	int matnum = Integer.valueOf(matnum_str);
	if(matnum%100!=0)
	{
	 System.out.println("The argument is incorrect.\n -matnum <number of matrix> that number must be divisible by 100.");
	 return;
	}
	
	//1. Create synthesized R matrix
	int num = matnum/100;
	JavaPairRDD<Long,Tuple2<Long,sLMatrixWritable>> Rrdd = ctx.textFile(inputpath)
	.repartition(100)
	.mapPartitions(new createRMat(matsize,num));;
	
	long numRmat = Rrdd.cache().count();
	//2. Compute TSQR but not build Q
	long start, end, s1, e1;
	start = new Date().getTime();
	
	String[] splitStr =  redsche_str.split(",");
	int[] redSche = new int[splitStr.length];
	int itrNum = redSche.length;
	for(int i=0;i<itrNum;i++)
	 redSche[i] = Integer.valueOf(splitStr[i]);

  JavaPairRDD<Long,Tuple2<Long,sLMatrixWritable>> pRrdd = Rrdd;
  
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	String Qspath = outputpath+"/Qs";
	fs.delete(new Path(Qspath),true);
  for(int i=0;i<itrNum-1;i++)
  {
   //gradually reduce number of partitions which is argument of "repartition" function
   pRrdd = pRrdd.repartition(redSche[i]).mapPartitions(new QRJob.mergedQR(i+1,Qspath)); 
  } 
    s1 = new Date().getTime();
	List<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>> RrddList = pRrdd.collect();
	e1 = new Date().getTime();
	cmDenseMatrix R;
	if(RrddList.size()>1){
	//merge Rf and then do QR factorization to get Qs and Rs
	int columns = RrddList.get(0)._2._2.matNumColumns();
	int rows = columns*RrddList.size();
	cmDenseMatrix mR = new cmDenseMatrix(rows,columns);
	Long[] keyIdArray = new Long[RrddList.size()];	
	
	long mergedId;
	int count = 0;
	s1 = new Date().getTime();
	for(Tuple2<Long,Tuple2<Long,sLMatrixWritable>> Rkvpair: RrddList)
	{
	 keyIdArray[count] = Rkvpair._2._1;
	 cmDenseMatrix Rv = Rkvpair._2._2.getDense();
	 //R matrix is square, its row size equal to column size
	 for(int i=0;i<columns;i++)
	 for(int j=0;j<columns;j++)
	 {
	  mR.set(i+count*columns,j,Rv.get(i,j));
	 }
	 count++;
	}
	e1 = new Date().getTime();
	
	mergedId = keyIdArray[0];
	s1 = new Date().getTime();
	QRF qrf2 = QRF.factorize(mR);
	e1 = new Date().getTime();
    cmDenseMatrix Qsmat = qrf2.getQ();
	cmDenseMatrix finalR = qrf2.getR();
	R = finalR;
	//split Qs and write the last Qs file 	
	SequenceFile.Writer swriter = new SequenceFile.Writer(fs,conf,new Path(Qspath+"/"+itrNum+"/"+mergedId),LongWritable.class,sMatrixWritable.class);
	LongWritable okey = new LongWritable();
	sMatrixWritable ovalue = new sMatrixWritable();
	
	long tt1, tt2, writingTime=0;
	for(int i=0;i<keyIdArray.length;i++)
	{
	 cmDenseMatrix Qsvalue = new cmDenseMatrix(columns,columns);
	 for(int j=0;j<columns;j++)
	 for(int k=0;k<columns;k++)
	 {
	  Qsvalue.set(j,k,Qsmat.get(j+columns*i,k));
	 }
	 //previous Q mergedId and last Q uId has same value, the last Id of Q use uId and previous Q use mergedId for key.
	 okey.set(keyIdArray[i]);
	 ovalue.set(Qsvalue);
	 tt1 = new Date().getTime();
	 swriter.append(okey,ovalue);
	 tt2 = new Date().getTime();
	 writingTime+=tt2-tt1;
	}
	System.out.println("Time of Writing :"+writingTime);
	swriter.close();
	}// if end: RrddList.size > 1
		
	end = new Date().getTime();
	long exectime = end-start;
	System.out.println("Running Time: "+exectime);
    System.exit(0);
  }
  
  private static class createRMat extends PairFlatMapFunction<Iterator<String>, Long, Tuple2<Long,sLMatrixWritable>>
 {
  private int matsize;
  private int matnum;
  
  public createRMat(int matsize,int matnum)
  {
   this.matsize = matsize;
   this.matnum = matnum;
  }
  
  public Iterable<Tuple2<Long, Tuple2<Long,sLMatrixWritable>>> call(Iterator<String> iter) {

	List<Tuple2<Long, Tuple2<Long,sLMatrixWritable>>> outputList = new ArrayList<Tuple2<Long, Tuple2<Long,sLMatrixWritable>>>();
	Random rnd = new Random();
	for(int i=0;i<matnum;i++)
	{
	 cmDenseMatrix rmat = new cmDenseMatrix(matsize,matsize);
	 for(int ir=0;ir<matsize;ir++)
	  for(int jr=ir;jr<matsize;jr++)
	   rmat.set(ir,jr,rnd.nextDouble());
	 long mergedId = rnd.nextLong();
	 outputList.add(new Tuple2<Long,Tuple2<Long,sLMatrixWritable>>(mergedId,new Tuple2<Long,sLMatrixWritable>(mergedId,new sLMatrixWritable(new long[1],rmat))));
	}
	return outputList;
	}
 }
 
}
