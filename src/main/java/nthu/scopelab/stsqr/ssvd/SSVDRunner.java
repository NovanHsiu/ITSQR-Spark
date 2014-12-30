package nthu.scopelab.stsqr.ssvd;
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
//Combine from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.SSVDRunner and org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver
// 2013 Hsiu-Cheng Yu
import java.util.Random;
import java.util.Date;
import java.util.List;
import java.lang.Math;
import scala.Tuple2;
import scala.collection.Iterator;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.scheduler.StageInfo;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.netlib.lapack.Dlarnv;

import nthu.scopelab.stsqr.TSQR;
import nthu.scopelab.stsqr.SerializeMatrixJob;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.DenseVector;

import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmUpperTriangDenseMatrix;
import nthu.scopelab.tsqr.math.EigenSolver;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;

public class SSVDRunner{
	
	private int outerBlockHeight;
	private int abtBlockHeight;
	
	private final JavaPairRDD<Long,sLMatrixWritable> Ardd;
	private final JavaSparkContext sc;
	private final String outputPath;
	private final int ablockRows;
	private final int k;
	private final int p;
	private int q;
	private String redSche;
	private final int vrbs;
	private boolean computeU = true;
	private boolean computeV = true;
	private boolean cUHalfSigma = false;
	private boolean cVHalfSigma = false;
	private boolean overwrite = true;
	private String uPath, vPath, sigmaPath;
	private Vector svaluesVector;
	//private boolean broadcast = true;
  
	public SSVDRunner(JavaPairRDD<Long,sLMatrixWritable> Ardd,JavaSparkContext sc,String outputpath,int k,int p,int r,int h,int abh,int q,
						int vrbs,String redSche,boolean computeU,boolean cUHalfSigma,boolean computeV,boolean cVHalfSigma,boolean overwrite)
	{
	 this.Ardd = Ardd;
	 this.sc = sc;
	 this.outputPath = outputpath;
	 this.k = k;
	 this.p = p;
	 this.ablockRows = r;
	 this.outerBlockHeight = h;
	 this.abtBlockHeight = abh;
	 this.q = q;
	 this.vrbs = vrbs;
	 this.computeU = computeU;
	 this.cUHalfSigma = cUHalfSigma;
	 this.computeV = computeV;
	 this.cVHalfSigma = cVHalfSigma;
	 this.overwrite = overwrite;
	 this.redSche = redSche;
	 uPath = outputPath+"/U";
	 vPath = outputPath+"/V";
	 sigmaPath = outputPath+"/Sigma";
	}
	
	public String getUPath()
	{
		return uPath;
	}
	
	public String getVPath()
	{
		return vPath;
	}
	
	public Vector getSigmaPath()
	{
		return svaluesVector;
	}
	
	public Vector getSigma()
	{
		return svaluesVector;
	}
	
	public void run() throws Exception
	{
	 Configuration conf = new Configuration();
	 FileSystem fs = FileSystem.get(conf);
     String btPath = outputPath+"/BtJob/";
	 
	 if (overwrite) {
        fs.delete(new Path(outputPath), true);
     }
	 //QJob	 
      int[] iseed = {0,0,0,1};
      double[] x = new double[1];
      Dlarnv.dlarnv(2,iseed,0,1,x,0);
      long seed = (long)(x[0]*(double)Long.MAX_VALUE);
	  
	  ssvdQRJob qrjob = new ssvdQRJob(sc,Ardd,seed,k,p,redSche);
	  qrjob.run();
	  JavaPairRDD<Long,sLMatrixWritable> Qrdd = qrjob.getQrdd().cache();
	  //BtJob
	  BtJob btjob = new BtJob(sc,Ardd,Qrdd,k,p,outerBlockHeight,btPath);
	  btjob.run();
	  cmUpperTriangDenseMatrix bbt = btjob.getBBt();
	  
	  //removed ABtDense iteration part temporarily
	  
	  // convert bbt to something our eigensolver could understand
      assert bbt.numColumns() == k + p;

      double[][] bbtSquare = new double[k + p][];
      for (int i = 0; i < k + p; i++) {
        bbtSquare[i] = new double[k + p];
      }

      for (int i = 0; i < k + p; i++) {
        for (int j = i; j < k + p; j++) {
          bbtSquare[i][j] = bbtSquare[j][i] = bbt.get(i, j);
        }
      }
      double[] svalues = new double[k + p];

      // try something else.
      EigenSolver eigenWrapper = new EigenSolver(bbtSquare);
	  	  
      double[] eigenva2 = eigenWrapper.getWR();
      for (int i = 0; i < k + p; i++) {
        svalues[i] = Math.sqrt(eigenva2[i]); // sqrt?
      }
      // save/redistribute UHat
      double[][] uHat = eigenWrapper.getVL();
	  
	  //uHat and svalues are necessary parameters for UJob and VJob
	  cmDenseMatrix uHatMat = new cmDenseMatrix(uHat);
	  svaluesVector = new DenseVector(svalues);
	  //UJob
	  UJob ujob = new UJob(Qrdd,uHatMat,svaluesVector,k,p,cUHalfSigma);
	  ujob.run();	  
	  //VJob
	  //read Btrdd
	  JavaPairRDD<IntWritable,VectorWritable> BtrddSeq = sc.hadoopFile(btPath,SequenceFileInputFormat.class,IntWritable.class,VectorWritable.class);
	  VJob vjob = new VJob(BtrddSeq,uHatMat,svaluesVector,k,p,vrbs,cVHalfSigma);
	  vjob.run();
	  	  
	  //output Urdd and Vrdd
	  ujob.getUrdd().saveAsHadoopFile(uPath,LongWritable.class,sLMatrixWritable.class,SequenceFileOutputFormat.class);
	  vjob.getVrdd().saveAsHadoopFile(vPath,LongWritable.class,sLMatrixWritable.class,SequenceFileOutputFormat.class);
	  //output sigma
	  SequenceFile.Writer svWriter =
        SequenceFile.createWriter(fs,
                                  fs.getConf(),
                                  new Path(sigmaPath+"/svalues.seq"),
                                  IntWritable.class,
                                  VectorWritable.class);

      svWriter.append(new IntWritable(0), new VectorWritable(new DenseVector(svalues, true)));
	  
      svWriter.close();
	}
	
	public static void main(String[] args) throws Exception {
	 //getArgument
	 String inputpath = TSQR.getArgument("-input",args);
    if (inputpath == null) {
            System.out.println("Required argument '-input' missing");
            return;
    }                
    String outputpath = TSQR.getArgument("-output",args);
    if (outputpath == null) {
            System.out.println("Required argument '-output' missing");
            return;
    }
	//decomposition rank
	String k_str = TSQR.getArgument("-rank",args);
	if (k_str == null) {
        System.out.println("Required argument '-rank' missing");
        return;
    }
	//oversampling
	String p_str = TSQR.getArgument("-oversampling",args);
	if (p_str == null) {
        p_str = "15";
    }
	//Y block height (must be > (k+p))
	String r_str = TSQR.getArgument("-blockHeight",args);
	if (r_str == null) {
        r_str = "10000";
    }
	//block height of outer products during multiplication, increase for sparse inputs
	String h_str = TSQR.getArgument("-outerProdBlockHeight",args);
	if (h_str == null) {
        h_str = "30000";
    }
	//block height of Y_i in ABtJob during AB' multiplication, increase for extremely sparse inputs
	String abh_str = TSQR.getArgument("-abtBlockHeight",args);
	if (abh_str == null) {
        abh_str = "200000";
    }
	String cu_str = TSQR.getArgument("-computeU",args);
	if (cu_str == null) {
        cu_str = "true";
    }
	//Compute U as UHat=U x pow(Sigma,0.5)
	String uhs_str = TSQR.getArgument("-uHalfSigma",args);
	if (uhs_str == null) {
        uhs_str = "false";
    }
	String cv_str = TSQR.getArgument("-computeV",args);
	if (cv_str == null) {
        cv_str = "true";
    }
	//compute V as VHat= V x pow(Sigma,0.5)
	String vhs_str = TSQR.getArgument("-vHalfSigma",args);
	if (vhs_str == null) {
        vhs_str = "false";
    }	
	//number of additional power iterations (0..2 is good)
	String q_str = TSQR.getArgument("-powerIter",args);
	if (q_str == null) {
        q_str = "0";
    }
	
	String srs_str = TSQR.getArgument("-subRowSize",args);
	if (srs_str == null) {
        System.out.println("Required argument '-subRowSize' missing");
        return;
    }
	
	String srsv_str = TSQR.getArgument("-vsubRowSize",args);
	if (srsv_str == null) {
        srsv_str = "100";
    }
	
	String rs_str = TSQR.getArgument("-reduceSchedule",args);
	if (rs_str == null) {
        rs_str = "1";
    }
	
    int k = Integer.parseInt(k_str);
    int p = Integer.parseInt(p_str);
    int r = Integer.parseInt(r_str);
    int h = Integer.parseInt(h_str);
    int abh = Integer.parseInt(abh_str);
    int q = Integer.parseInt(q_str);
    boolean computeU = Boolean.parseBoolean(cu_str);
    boolean computeV = Boolean.parseBoolean(cv_str);
    boolean cUHalfSigma = Boolean.parseBoolean(uhs_str);
    boolean cVHalfSigma = Boolean.parseBoolean(vhs_str);   
	int subRowSize = Integer.parseInt(srs_str);
	int subRowSizeV = Integer.parseInt(srsv_str);
	
    boolean overwrite = true;
	
	//Setup SparkContext
	String exememory = TSQR.getArgument("-em",args);
	if(exememory==null)
	 exememory = "512m";
	String master = TSQR.getArgument("-master",args);
	if(master==null)
	{
	 System.out.println("Required argument '-master' missing");
     return;
	}
	
	String sparkHome = System.getenv("SPARK_HOME");
	SparkConf sconf = new SparkConf().setMaster(master)
	.setAppName("SSVD")
	.setSparkHome(sparkHome)
	.setJars(new String[]{sparkHome+"/oproject/target/simple-project-1.0.jar"})
	.set("spark.executor.memory", exememory);
    JavaSparkContext ctx = new JavaSparkContext(sconf);
	Thread.sleep(2000);
	long start, end;
	start = new Date().getTime();
	//Produce RDD of matrix A
	SerializeMatrixJob matrixrdd = new SerializeMatrixJob(ctx,new Integer(subRowSize),inputpath);
	JavaPairRDD<Long,sLMatrixWritable> Ardd = matrixrdd.run();
	Ardd.setName("Ardd");
    SSVDRunner ssvdrunner = new SSVDRunner(Ardd,ctx,outputpath,k,p,r,h,abh,q,subRowSizeV,rs_str,computeU,cUHalfSigma,computeV,cVHalfSigma,overwrite);	 
	 ssvdrunner.run();
	 end = new Date().getTime();
	 System.out.println("Finished in: "+(end-start));
	}
	
}
