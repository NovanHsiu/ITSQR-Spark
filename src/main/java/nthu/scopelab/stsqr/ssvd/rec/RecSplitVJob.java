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

import java.util.List;
import java.util.Date;
import java.util.ArrayList;
import java.io.IOException;

public class RecSplitVJob{

 private JavaSparkContext sc;
 private String upath;
 private String vpath;
 private String outputpath;
 Vector Sigma;
 private int thitem;
 private int numRecommendations;
 private int vsplit; //split number of v file
 private String[][] vFile; //split v file for each computation iteration
  
 public RecSplitVJob(JavaSparkContext sc,String UPath, String outputPath, String vPath, Vector Sigma, int thItem, int numRecommendations, int vsplit)
 {
  this.sc = sc;
  this.upath = UPath;
  this.outputpath = outputPath;
  this.vpath = vPath;
  this.Sigma = Sigma;
  this.thitem = thItem;
  this.numRecommendations = numRecommendations;
  this.vsplit = vsplit;
  //count v file and split it which depend on vsplit
 }
 
 public RecSplitVJob(JavaSparkContext sc, String UPath, String outputPath, String vPath, String sigmaPath, int thItem, int numRecommendations, int vsplit)
 {
  this.sc = sc;
  this.upath = UPath;
  this.outputpath = outputPath;
  this.vpath = vPath;
  this.thitem = thItem;
  this.numRecommendations = numRecommendations;
  this.vsplit = vsplit;
  //read Sigma from hdfs
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
 
 public void run() throws IOException
 {
  //read v, and divide it by "vsplit"
  FileSystem fs = FileSystem.get(new Configuration());
  FileStatus[] vfileStatus = fs.listStatus(new Path(vpath),new QRFactorMultiply.MyPathFilter("part"));
  //check the size of v file. if size > 0 add to List for divide
  //check
  List<String> vfileList = new ArrayList<String>();
  for(int i=0;i<vfileStatus.length;i++)
  {
   if(vfileStatus[i].getBlockSize() > 0)
    vfileList.add(vfileStatus[i].getPath().toString());
  }
  
  vFile = new String[vsplit][];
  int dividedNum = vfileList.size()/vsplit;
  //divided
  for(int i=0;i<vsplit;i++)
  {
   if(i<vsplit-1)
    vFile[i] = new String[dividedNum];
   else //the final part
   {
    dividedNum = vfileList.size();
    vFile[i] = new String[dividedNum];
   }
   int j = 0;
   while(j<dividedNum)
   {
    vFile[i][j] = vfileList.remove(0);
    j++;
   }
  }
  //sigma^3
  for(int i=0;i<Sigma.size();i++)
	 Sigma.set(i,Sigma.get(i)*Sigma.get(i)*Sigma.get(i));
  //urdd do cartesian transformation for vrdd that would produce a rdd which have all u and v partitions pair
  JavaPairRDD<LongWritable,sLMatrixWritable> urdd = sc.hadoopFile(upath,SequenceFileInputFormat.class,LongWritable.class,sLMatrixWritable.class);

  List<JavaPairRDD<Long,RecommendedItemsWritable>> uvrddList = new ArrayList<JavaPairRDD<Long,RecommendedItemsWritable>>();
  //start = new Date().getTime();
  //compute U x Sigma^3 x Vt
  for(int i=0;i<vsplit;i++)
  {
   uvrddList.add(urdd.flatMap(new RecommendationFunction(Sigma,thitem,numRecommendations,vFile[i],i)));
  }
  //combine uvrdd and select top k item
   
   int mergenum = uvrddList.size();
   while(mergenum>1)
   {
    boolean odd = false;
	if(mergenum%2!=0)
	 odd = true;
    	for(int i=0;i<mergenum;i=i+2)
	{
	 if(odd && i==mergenum-1)
	  uvrddList.set(i/2,uvrddList.get(i));
	 else
	  uvrddList.set(i/2,uvrddList.get(i).join(uvrddList.get(i+1)).map(new SelectTopkFunction(numRecommendations)));
	}
	mergenum = mergenum/2;
	if(odd)
	 mergenum+=1;
   }
   JavaPairRDD<LongWritable,RecommendedItemsWritable> outputRdd = uvrddList.get(0).map(new outputTransform());
   //output the result
   outputRdd.saveAsHadoopFile(outputpath,LongWritable.class,RecommendedItemsWritable.class,TextOutputFormat.class);
 }
  
 private static class RecommendationFunction extends PairFlatMapFunction<Tuple2<LongWritable,sLMatrixWritable>, Long, RecommendedItemsWritable>
 {
  private String vpath;
  private Vector sigma3;
  private int threshold;
  private int numrec;
  private String[] vFile;
  private int order;  

  public RecommendationFunction(Vector sigma3,int thitem,int numrec,String[] vFile,int order)
  {
   this.sigma3 = sigma3;
   this.threshold = thitem;
   this.numrec = numrec;
   this.vFile = vFile;
   this.order = order;
  }
  
  public Iterable<Tuple2<Long,RecommendedItemsWritable>> call(Tuple2<LongWritable,sLMatrixWritable> upair) throws IOException {
	 System.out.println(order);
	 //initial arguments
	 cmDenseMatrix outputMat = null;
	 FileSystem fs = FileSystem.get(new Configuration());
	 List<Tuple2<Long,RecommendedItemsWritable>> outputList = new ArrayList<Tuple2<Long,RecommendedItemsWritable>>();
	 double d = 0.0;
	 int index = 0;
	 int m, n;
	 LongWritable vkey = new LongWritable();
	 sLMatrixWritable vvalue = new sLMatrixWritable();
	 cmDenseMatrix UMat = upair._2.getDense();
	 m = UMat.numRows();
	 n = UMat.numColumns();//sigmaMatrix.numColumns();

	 //U x(sigma^3) use dscal to doing matrix multiplication
	 for(int i=0;i<n;i++)
	  Dscal.dscal(m,sigma3.get(i),UMat.getData(),i*m,1);
	
	long[] userIndex = upair._2.getLongArray();
	int userIndexLength = upair._2.getLongLength();
	List<TopkItem> topkItemList = new ArrayList<TopkItem>();
	for(int i=0;i<userIndexLength;i++)
	 topkItemList.add(new TopkItem(numrec));
	 
    // read V ,compute U*Vt and offer to topkItemList
	SequenceFile.Reader sreader;
	for(int vi=0;vi<vFile.length;vi++)
	{
	  sreader = new SequenceFile.Reader(fs,new Path(vFile[vi]),fs.getConf());
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
	  
	  outputMat = QRFactorMultiply.Multiply("N","T",UMat,VMat,outputMat);
	  long[] itemIndex = vvalue.getLongArray();
	  
	  //select top k item from recommendationVector
	  for(int i=0;i<m;i++)
	  {
	   Vector recommendationVector = new SparseVector(Integer.MAX_VALUE);
	   for(int j=0;j<n;j++)
	   {
		d = outputMat.getData()[j*m+i];
		index = (int) itemIndex[j];
		
		//recommendationVector.set( index , d ); //testing
	    if( d > threshold )
		{
	        recommendationVector.set( index , (double) (d/threshold) );
			//System.out.println("test threshold!");
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
	  }//while: sreader
	  sreader.close();
	}//for: vFile.length
	
	 //add the recommendation into List and return it
	 for(int i=0;i<m;i++)
	 {
	   /*if (topkItemList.get(i).numItem()>0)
	   {
	   }*/
	   outputList.add(new Tuple2<Long,RecommendedItemsWritable>(userIndex[i], new RecommendedItemsWritable(topkItemList.get(i))));
	 }
	return outputList;
	}//function call
  }
  
 private static class SelectTopkFunction extends PairFunction<Tuple2<Long,Tuple2<RecommendedItemsWritable,RecommendedItemsWritable>>, Long, RecommendedItemsWritable>
 {
  private int numrec;
  
  public SelectTopkFunction(int numrec)
  {
   this.numrec = numrec;
  }
  
  public Tuple2<Long,RecommendedItemsWritable> call(Tuple2<Long,Tuple2<RecommendedItemsWritable,RecommendedItemsWritable>> kvpair) throws IOException {
	 
	 TopkItem outputTopK = new TopkItem(numrec);
	 RecommendedItemsWritable rec1 = kvpair._2._1;
	 RecommendedItemsWritable rec2 = kvpair._2._2;
	 outputTopK = TopkOffer(outputTopK,rec1);
	 outputTopK = TopkOffer(outputTopK,rec2);
	 	 
	 return new Tuple2<Long,RecommendedItemsWritable>(kvpair._1, new RecommendedItemsWritable(outputTopK));
	}//function call
	
	public TopkItem TopkOffer(TopkItem outputTopK,RecommendedItemsWritable rec)
	{
	 TopkItem topk = rec.get();
	 int numitem = topk.numItem();
	 for(int i=0;i<numitem;i++)
	 {
	  outputTopK.offer(topk.get(i));
	 }
	 return outputTopK;
	}
  }
  
  private static class outputTransform extends PairFunction<Tuple2<Long,RecommendedItemsWritable>, LongWritable, RecommendedItemsWritable>
 { 
  public Tuple2<LongWritable,RecommendedItemsWritable> call(Tuple2<Long,RecommendedItemsWritable> kvpair) throws IOException {	 	 
	 return new Tuple2<LongWritable,RecommendedItemsWritable>(new LongWritable(kvpair._1), kvpair._2);
	}//function call	
  }
  
 }

