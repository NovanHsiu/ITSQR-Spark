package nthu.scopelab.stsqr.ssvd;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.math.QRF;
import nthu.scopelab.tsqr.math.QRFactorMultiply;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import nthu.scopelab.tsqr.matrix.cmSparseMatrix;
import nthu.scopelab.tsqr.ssvd.Omega;

import nthu.scopelab.stsqr.QRJob;

public class ssvdQRJob{

	private JavaSparkContext sc;
  private JavaPairRDD<Long,sLMatrixWritable> matrdd;
  private JavaPairRDD<Long,sLMatrixWritable> Qrdd;
  private int[] reduceSchedule;
  private static Omega omega;
  private static int kp; 
  
  public ssvdQRJob(JavaSparkContext sc,JavaPairRDD<Long,sLMatrixWritable> matrdd, long omegaSeed, int k, int p, String redSche_str)
  {
   this.sc = sc;
   this.matrdd = matrdd;
   omega = new Omega(omegaSeed, k, p);
   kp = k+p;
   String[] splitStr = redSche_str.split(",");
   reduceSchedule = new int[splitStr.length];
   for(int i=0;i<splitStr.length;i++)
	reduceSchedule[i] = Integer.valueOf(splitStr[i]);
  }
  
  public void run() throws IOException
  {
	JavaPairRDD<Long,sLMatrixWritable> Yrdd = matrdd.map(new computeYrdd(omega,kp));
	//store Qs in HDFS
	QRJob IQR = new QRJob(sc,Yrdd);
	IQR.run(reduceSchedule); //IterNum
	Qrdd = IQR.getQrdd();
  }
  
  protected static class computeYrdd extends PairFunction<Tuple2<Long,sLMatrixWritable>, Long, sLMatrixWritable>{
	
	private Omega omega;
	private int kp;
	private int tcount = 0;
	
	public computeYrdd(Omega omega,int kp)
	{
	 this.omega = omega;
	 this.kp = kp;
	}
	
	@Override
	public Tuple2<Long,sLMatrixWritable> call(Tuple2<Long,sLMatrixWritable> kvpair) {
		return new Tuple2<Long, sLMatrixWritable>(kvpair._1, computeY(kvpair._2,omega,kp));
      }
  }
  
  private static sLMatrixWritable computeY(sLMatrixWritable lmat,Omega omega,int kp)
  {
	cmSparseMatrix subAs = null;
	cmDenseMatrix subAd = null;
	  int subANumRows = -1;
	  if(!lmat.isDense())
	  {
		subAs = lmat.getSparse();
		subANumRows = subAs.numRows();
	  }
	  else
	  {
	    subAd = lmat.getDense();
		subANumRows = subAd.numRows();
	  }
	  
	  cmDenseMatrix subY = new cmDenseMatrix(subANumRows,kp);
	   
	  //get the Y sub matrix  from A sub matrix * omega
	  //computeY
	   if(!lmat.isDense())
	   {
	    omega.computeY(subAs,subY);
	   }
	   else
	   {
	    omega.computeY(subAd,subY);
	   }
	return new sLMatrixWritable(lmat.getLongArray(),subY);
  }
  
  public JavaPairRDD<Long,sLMatrixWritable> getQrdd()
  {
   return Qrdd;
  }
  
}
