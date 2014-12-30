package nthu.scopelab.stsqr.ssvd;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import org.apache.hadoop.io.LongWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.math.QRFactorMultiply;

import java.io.IOException;

import no.uib.cipr.matrix.Vector;

public class UJob{

  private JavaPairRDD<Long,sLMatrixWritable> Qrdd;
  private JavaPairRDD<LongWritable,sLMatrixWritable> Urdd;
  private int k, kp;
  private cmDenseMatrix Uhat;
  
  public UJob(JavaPairRDD<Long,sLMatrixWritable> Qrdd,cmDenseMatrix Uhat,Vector sValues,int k,int p,boolean uHalfSigma)
  {
	this.Qrdd = Qrdd;
	this.Uhat = Uhat;
	this.k = k;
	this.kp = k+p;
	
	if(k!=kp)
	{
	   cmDenseMatrix pre_uHat = Uhat;
	   this.Uhat = new cmDenseMatrix(new double[pre_uHat.numRows()*k],Uhat.numRows(),k);
	   for(int i=0;i<this.Uhat.numRows();i++)
	    for(int j=0;j<this.Uhat.numColumns();j++)
		 this.Uhat.set(i,j,pre_uHat.get(i,j));
	}
	
	if (uHalfSigma) {
        for (int i = 0; i < k; i++) {
          sValues.set(i, Math.sqrt(sValues.get(i)));
        }
		for(int i=0;i<Uhat.numRows();i++)
	     for(int j=0;j<Uhat.numColumns();j++)
		  Uhat.set(i,j,Uhat.get(i,j)*sValues.get(j));
    }
	
  }
  
  public JavaPairRDD<LongWritable,sLMatrixWritable> getUrdd()
  { 
   return Urdd;
  }
  
  public void run() throws IOException
  {
   //Q*Uhat
   Urdd = Qrdd.map(new UMapFunction(Uhat));
  }
  
  protected static class UMapFunction extends PairFunction<Tuple2<Long,sLMatrixWritable>,LongWritable,sLMatrixWritable> {
	
	private cmDenseMatrix uHat;
	private int qrow, uhcol;
	
	public UMapFunction(cmDenseMatrix uHat)
	{
	 this.uHat = uHat;
	}
	
	public Tuple2<LongWritable,sLMatrixWritable> call(Tuple2<Long,sLMatrixWritable> kvpair) throws IOException {
	
		qrow = kvpair._2.getDense().numRows();
		uhcol = uHat.numColumns();
		
		cmDenseMatrix Umat = new cmDenseMatrix(qrow,uhcol);
			 
		Umat = QRFactorMultiply.Multiply("N","N",kvpair._2.getDense(),uHat,Umat);
		
		return new Tuple2<LongWritable,sLMatrixWritable>(new LongWritable(kvpair._1),new sLMatrixWritable(kvpair._2.getLongArray(),Umat));
    }
	 
  }
    
}
