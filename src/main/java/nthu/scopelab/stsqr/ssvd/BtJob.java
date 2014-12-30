package nthu.scopelab.stsqr.ssvd;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.cmUpperTriangDenseMatrix;

import java.util.List;
import java.util.Date;
import java.util.ArrayList;
import java.security.InvalidAlgorithmParameterException;
import java.util.Iterator;
import java.io.IOException;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.DenseVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;//.Writer;

import nthu.scopelab.tsqr.matrix.cmSparseMatrix;

public class BtJob{

  private JavaPairRDD<Long,sLMatrixWritable> Ardd;
  private JavaPairRDD<Long,sLMatrixWritable> Qrdd;
  private JavaSparkContext sc;
  private int kp, height;
  private String BtPath;
  private cmUpperTriangDenseMatrix BBt=null;
  private static boolean debug = true;
  
  public BtJob(JavaSparkContext sc,JavaPairRDD<Long,sLMatrixWritable> Ardd,JavaPairRDD<Long,sLMatrixWritable> Qrdd, int k,int p,
                int outerBlockHeight,String BtPath)
  {
   this.Ardd = Ardd;
   this.Qrdd = Qrdd;
   this.BtPath = BtPath;
   this.height = outerBlockHeight;
   this.sc = sc;
   kp = k+p;
   try
   {
   if(k<=0 || p<0)
	 throw new InvalidAlgorithmParameterException("invalid parameter p or k!");
	}
	catch(Exception e)
	{
	 e.printStackTrace();
	}
  }
  
  public cmUpperTriangDenseMatrix getBBt()
  { 
   return BBt;
  }
  
  public void run() throws IOException
  {
   //map: join Ardd and Qrdd and map with BtMapFunction  
   JavaPairRDD<Integer,SparseRowBlock> Btm = Ardd.join(Qrdd).flatMap(new BtMapFunction(kp,height));
   //reduce: compute Bt. output Bt to the hdfs and get the BBtrdd
   //FileSystem fs = FileSystem.get(new Configuration());
   JavaPairRDD<Integer,Double[]> BBtrdd = Btm.groupByKey().map(new BtReduceFunction(kp,height,BtPath));
   
   //accumulate and compute BBt
   List<Tuple2<Integer,Double[]>> BBtList = BBtrdd.collect();
   double[] dBBt = new double[(kp*(kp+1))/2]; //number of non-zero element of size kp u.t. dense matrix 
   for(Tuple2<Integer,Double[]> bbtpair : BBtList)
   {
    Double[] sbbt = bbtpair._2;
    for(int i=0;i<sbbt.length;i++)
     dBBt[i]+=sbbt[i];
   }
   
   BBt = new cmUpperTriangDenseMatrix(dBBt);
  }
  
  protected static class BtMapFunction extends PairFlatMapFunction<Tuple2<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>>,Integer,SparseRowBlock> {
	
	private int kp, height;
	private long t1, t2;
	public BtMapFunction(int kp, int height)
	{
	 this.kp = kp;
	 this.height = height;
	}
	
	public Iterable<Tuple2<Integer,SparseRowBlock>> call(Tuple2<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>> kvpair) throws IOException {
		t1 = new Date().getTime();
		SparseRowBlockAccumulator btCollector = new SparseRowBlockAccumulator(height);
		
		sLMatrixWritable Amat = kvpair._2._1;
		cmDenseMatrix Q = kvpair._2._2.getDense();
		int m = Amat.matNumRows();
		
		for(int i=0;i<m;i++)
		{
			Vector btRow;
			 if(Amat.isDense())
			 {
			  //A is dense matrix
			  // maybe in this part, we could use QRFactorMultiply.Multiply (blas matrix multiplication)
			  double[] dARow = Amat.getDense().getRow(i);			  
			  for(int j=0;j<dARow.length;j++)
			  {
			   btRow = new DenseVector(kp);
			   for (int k = 0; k < kp; k++)
			   {
			    btRow.set(k, dARow[j] * Q.get(i,k));
			   }
			   btCollector.collect(j, btRow);
			  }	
			 }
			 else
			 {
			 //A is sparse matrix
			 cmSparseMatrix As = Amat.getSparse();
			 Vector aRow = As.getRow(i);
			 for (VectorEntry ev: aRow)
			 {
			  double mul = ev.get();
			  btRow = new DenseVector(kp);
			  for (int j = 0; j < kp; j++)
				btRow.set(j, mul * Q.get(i,j));
			  btCollector.collect(ev.index(), btRow);
			 }
			}
			//column
		}//for i row
		t2 = new Date().getTime();
		//if(debug)
			//System.out.println("Bt ComputeTime: "+(t2-t1));
		return btCollector.getBlockList();
    }
	 
  }
  
  protected static class BtReduceFunction extends PairFunction<Tuple2<Integer,List<SparseRowBlock>>,Integer,Double[]> {
	protected int blockHeight;   
	private int kp;		
	private String BtPath;
	
	public BtReduceFunction(int kp, int height, String BtPath) throws IOException
	{
	 this.kp = kp;
	 this.blockHeight = height;
	 this.BtPath = BtPath;
	}
	
	public Tuple2<Integer,Double[]> call(Tuple2<Integer,List<SparseRowBlock>> kvpair) throws IOException
	{
	 SparseRowBlock accum = new SparseRowBlock();
	 
	 cmUpperTriangDenseMatrix mBBt = new cmUpperTriangDenseMatrix(kp);
	 
	 List<SparseRowBlock> blockList = kvpair._2;
	 for(SparseRowBlock block : blockList)
	 {
      accum.plusBlock(block);
	 }
	  
	 FileSystem fs = FileSystem.get(new Configuration());
	 SequenceFile.Writer swriter = new SequenceFile.Writer(fs,fs.getConf(),new Path(BtPath+Integer.toString(kvpair._1)),IntWritable.class,VectorWritable.class);
	 for (int k = 0; k < accum.getNumRows(); k++) {
        Vector btRow = accum.getRows()[k];
        int rowIndex = kvpair._1 * blockHeight + accum.getRowIndices()[k];
		
		//Use the SequenceWriter write this pair to hdfs. Use input key be the Bt file filename.
		swriter.append(new IntWritable(rowIndex),new VectorWritable((DenseVector)btRow));
		
          int kp = mBBt.numRows();
          // accumulate partial BBt sum
          for (int i = 0; i < kp; i++) {	    
            double vi = btRow.get(i);
            if (vi != 0.0) {
              for (int j = i; j < kp; j++) {
                double vj = btRow.get(j);
                if (vj != 0.0) {
                  mBBt.set(i, j, mBBt.get(i, j) + vi * vj);
                }
              }
            }
          }//compute BBt
      }//compute Bt
	  swriter.close();
	  
	  Double[] dBBt = new Double[mBBt.getData().length];
	  for(int i=0;i<dBBt.length;i++)
	   dBBt[i] = mBBt.getData()[i];
	  return new Tuple2<Integer,Double[]>(kvpair._1,dBBt);
	}
  
  }
  
}
