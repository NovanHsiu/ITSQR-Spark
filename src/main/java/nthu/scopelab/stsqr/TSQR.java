package nthu.scopelab.stsqr;

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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

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

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.math.QRF;
import nthu.scopelab.tsqr.math.QRFactorMultiply;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

public class TSQR {
  
  public static void main(String[] args) throws Exception {
    String projectdir = getArgument("-projectDir",args);
	checkArgument(projectdir);
	String inputpath = getArgument("-input",args);
	checkArgument(inputpath);
	String master = getArgument("-master",args);
	checkArgument(master);
	String outputpath = getArgument("-output",args);
	if(outputpath==null)
	 outputpath = "output";
	String exememory = getArgument("-em",args);
	if(exememory==null)
	 exememory = "512m";
	String strrbs = getArgument("-subRowSize",args);
	checkArgument(strrbs);
	String rs_str = TSQR.getArgument("-reduceSchedule",args);
	if (rs_str == null) {
        rs_str = "1";
    }
	
	long start, end;
	start = new Date().getTime();
    	
	String sparkHome = System.getenv("SPARK_HOME");
	SparkConf sconf = new SparkConf().setMaster(master)
	.setAppName("TSQR")
	.setSparkHome(System.getenv("SPARK_HOME"))
	.setJars(new String[]{projectdir+"/target/simple-project-1.0.jar"})
	.set("spark.executor.memory", exememory);
    JavaSparkContext ctx = new JavaSparkContext(sconf);
    // 1. prepartionJob: turn matrix from text(string) to hadoop sequencefile(sLMatrixWritable)
    	Thread.sleep(2000);	
	 SerializeMatrixJob matrixrdd = new SerializeMatrixJob(ctx,new Integer(strrbs),inputpath);
	 JavaPairRDD<Long,sLMatrixWritable> submat = matrixrdd.run();//.cache();
	 
	String[] splitStr = rs_str.split(",");
    int[] reduceSchedule = new int[splitStr.length];
    for(int i=0;i<splitStr.length;i++)
	 reduceSchedule[i] = Integer.valueOf(splitStr[i]);
	
	QRJob QR = new QRJob(ctx,submat);
	QR.run(reduceSchedule);
	
	JavaPairRDD<Long,sLMatrixWritable> Q = QR.getQrdd();
	cmDenseMatrix finalR = QR.getR();
	
	JavaPairRDD<LongWritable,sLMatrixWritable> output = Q.map(
	new PairFunction<Tuple2<Long,sLMatrixWritable>,LongWritable,sLMatrixWritable>(){
	public Tuple2<LongWritable,sLMatrixWritable> call(Tuple2<Long,sLMatrixWritable> kv)
	{
	 return new Tuple2<LongWritable,sLMatrixWritable>(new LongWritable(kv._1),kv._2);
	}
	}
	);
	output.saveAsHadoopFile(outputpath,LongWritable.class,sLMatrixWritable.class,SequenceFileOutputFormat.class);
	
	
	//verifiy QR = A and do Q*R - A
	/*JavaPairRDD<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>> AQ = submat.join(Q);
	JavaPairRDD<Long,Double> verfication = AQ.map(new verifyQR(finalR));
	List<Tuple2<Long,Double>> vlist = verfication.collect();
	for(Tuple2<Long,Double> vt : vlist)
	{
	 System.out.println("key: "+vt._1);
	 System.out.println(vt._2);
	}*/
	
	end = new Date().getTime();
	long exectime = end-start;
	System.out.println("Ececution Time: "+exectime);
    System.exit(0);
  }
  
  static class verifyQR extends PairFunction<Tuple2<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>>,Long,Double>
  {
   cmDenseMatrix R;
   public verifyQR(cmDenseMatrix mat)
   {
    R = mat;
   }
   public Tuple2<Long,Double> call(Tuple2<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>> kvpair) {
	cmDenseMatrix A = kvpair._2._1.getDense();
	cmDenseMatrix Q = kvpair._2._2.getDense();
	//Multiply
	cmDenseMatrix QR = new cmDenseMatrix(Q.numRows(),R.numColumns());
	QR = QRFactorMultiply.Multiply("N","N",Q,R,QR);
	//do As = A - QR, and then sum all elements of As
	double sum = 0.0;
	for(int i = 0;i<A.numRows();i++)
	 for(int j = 0;j<A.numColumns();j++)
	 {
	  sum+=Math.abs(Math.abs(A.get(i,j))-Math.abs(QR.get(i,j)));
	 }
	//return Result
	return new Tuple2<Long,Double>(kvpair._1,sum);
   }
  }
  
    public static String getArgument(String arg, String[] args) {
        for (int i=0; i<args.length; ++i) {
            if (arg.equals(args[i])) {
                if (i+1<args.length) {
                    return args[i+1];
                } else {
                    return null;
                }
            }
        }
        return null;
    }
	
	public static void checkArgument(String arg)
	{
	 if(arg==null)
	 {
	  System.out.println("Usage: run-oproject TSQR -projectDir <project directory> -input <inputpath> -master <master> -rbs <row block size> [-output <outputpath> -em <executor_memory>]");
	  System.exit(0);
	 }
	}
}
