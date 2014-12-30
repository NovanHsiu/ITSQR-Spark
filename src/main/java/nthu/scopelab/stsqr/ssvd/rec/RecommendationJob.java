package nthu.scopelab.stsqr.ssvd.rec;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.TridiagMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.math.QRFactorMultiply;

import org.netlib.blas.Dscal;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class RecommendationJob{

 private JavaSparkContext sc;
 private String inputpath;
 private String outputpath;
 private String vpath;
 Vector Sigma;
 private int thitem;
 private int numRecommendations;
  
 public RecommendationJob(JavaSparkContext sc,String UPath, String outputPath, String vPath, Vector Sigma, int thItem, int numRecommendations)
 {
  this.sc = sc;
  this.inputpath = UPath;
  this.outputpath = outputPath;
  this.vpath = vPath;
  this.Sigma = Sigma;
  this.thitem = thItem;
  this.numRecommendations = numRecommendations;
 }
 
 public RecommendationJob(JavaSparkContext sc, String UPath, String outputPath, String vPath, String sigmaPath, int thItem, int numRecommendations)
 {
  this.sc = sc;
  this.inputpath = UPath;
  this.outputpath = outputPath;
  this.vpath = vPath;
  this.thitem = thItem;
  this.numRecommendations = numRecommendations;
  //import fs, conf, SequenceFile
  try{
		FileSystem fs = FileSystem.get(new Configuration());
        SequenceFile.Reader sreader = new SequenceFile.Reader(fs, new Path(sigmaPath), fs.getConf());
        IntWritable key = new IntWritable();
        VectorWritable value = new VectorWritable();
        if (sreader.next( key,value )){
            Sigma = value.get();
		}
		sreader.close();
  }
  catch(Exception e)
  {
   e.printStackTrace();
  }
 }
 
 public void run()
 {
  //input U, use SequenceFile.Reader read V, 
  JavaPairRDD<LongWritable,RecommendedItemsWritable> outputRdd = sc.hadoopFile(inputpath,SequenceFileInputFormat.class,LongWritable.class,sLMatrixWritable.class).
  flatMap(new RecommendationFunction(vpath,Sigma,thitem,numRecommendations));
  outputRdd.saveAsHadoopFile(outputpath,LongWritable.class,RecommendedItemsWritable.class,TextOutputFormat.class);
 }
  
 private static class RecommendationFunction extends PairFlatMapFunction<Tuple2<LongWritable,sLMatrixWritable>, LongWritable, RecommendedItemsWritable>
 {

  private String vpath;
  private Vector sigma;
  private int threshold;
  private int numrec;
  private cmDenseMatrix outputMat = null;
  private long total, selection, s1, e1, s2, e2;
  
  public RecommendationFunction(String vpath,Vector sigma,int thitem,int numrec)
  {
   this.vpath = vpath;
   this.sigma = sigma;
   this.threshold = thitem;
   this.numrec = numrec;
	//sigma^3
	for(int i=0;i<sigma.size();i++)
	 sigma.set(i,sigma.get(i)*sigma.get(i)*sigma.get(i));
  }
  
  public Iterable<Tuple2<LongWritable,RecommendedItemsWritable>> call(Tuple2<LongWritable,sLMatrixWritable> kvpair) throws IOException {
	 s1 = new Date().getTime();//test
	 //initial arguments
	 List<Tuple2<LongWritable,RecommendedItemsWritable>> outputList = new ArrayList<Tuple2<LongWritable,RecommendedItemsWritable>>();
	 LongWritable vkey = new LongWritable();
	 sLMatrixWritable vvalue = new sLMatrixWritable();
	 FileSystem fs = FileSystem.get(new Configuration());
	 double d = 0.0;
	 int index = 0;
	 int m, n;
	 cmDenseMatrix inputMat = kvpair._2.getDense();
	 m = inputMat.numRows();
	 n = inputMat.numColumns();//sigmaMatrix.numColumns();

	 //U x(sigma^3) use dscal to doing matrix multiplication
	 for(int i=0;i<n;i++)
	  Dscal.dscal(m,sigma.get(i),inputMat.getData(),i*m,1);
    
	//construct v squencefile reader
	FileStatus[] vfileStatus = fs.listStatus(new Path(vpath),new QRFactorMultiply.MyPathFilter("part"));	
	SequenceFile.Reader sreader;

	long[] userIndex = kvpair._2.getLongArray();
	int userIndexLength = kvpair._2.getLongLength();
	List<TopkItem> topkItemList = new ArrayList<TopkItem>();
	for(int i=0;i<userIndexLength;i++)
	 topkItemList.add(new TopkItem(numrec));
	 
	//results matrix row size
	m = inputMat.numRows(); 
    try{
	for(int vi=0;vi<vfileStatus.length;vi++)
	{
	  sreader = new SequenceFile.Reader(fs,vfileStatus[vi].getPath(),fs.getConf());
	  while(sreader.next(vkey,vvalue))
	  {
	  cmDenseMatrix VMat = vvalue.getDense();
	  
	  // (U x  (sigma^3)) x V' 
	  //results matrix col size
	  n = VMat.numRows();
	  if(outputMat==null)
	  {
	   outputMat = new cmDenseMatrix(new double[m*n*2],m,n);
	  }
	  else if(outputMat.numRows()*outputMat.numColumns()<m*n)
	   outputMat = new cmDenseMatrix(new double[m*n*2],m,n);
	  
	  outputMat = QRFactorMultiply.Multiply("N","T",inputMat,VMat,outputMat);
	  long[] itemIndex = vvalue.getLongArray();
	  
	  //select top k item from recommendationVector
	  s2 = new Date().getTime();//test
	  for(int i=0;i<m;i++)
	  {
	   Vector recommendationVector = new SparseVector(Integer.MAX_VALUE);
	   for(int j=0;j<n;j++)
	   {
		d = outputMat.getData()[j*m+i];
		index = (int) itemIndex[j];
	    if( d > threshold )
		{
	        recommendationVector.set( index , (double) (d/threshold) );
		}
	   }//for 2 item
		
		for (VectorEntry ve : recommendationVector) {
		 index = ve.index();
		 long itemID;
		 //we don't have any mappings, so just use the original
		 itemID = index;
		 double dvalue = ve.get();
		 if (!Double.isNaN(dvalue)) {
          topkItemList.get(i).offer(new Tuple2<Long,Double>(itemID, dvalue));
         }
		}
	  }//for 1 user	  
	  e2 = new Date().getTime();//test
	  selection += (e2 - s2);//test
	  }//while: sreader
	  sreader.close();
	}//for: vfilestatus
	System.out.println("Selection Time: "+selection);//test
	 for(int i=0;i<m;i++)
	 {
	   if (topkItemList.get(i).numItem()>0)
	   {
	    outputList.add(new Tuple2<LongWritable,RecommendedItemsWritable>(new LongWritable(userIndex[i]), new RecommendedItemsWritable(topkItemList.get(i))));
	   }
	 }
    }//try
	catch (Exception e){
	 e.printStackTrace();
    }
	e1 = new Date().getTime();//test
	total = e1 - s1;//test
	System.out.println("Total Time: "+total);//test
	return outputList;
  }//call function
  
 }
 
}
