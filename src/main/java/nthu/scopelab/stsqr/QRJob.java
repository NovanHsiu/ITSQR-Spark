package nthu.scopelab.stsqr;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sMatrixWritable;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.math.QRF;
import nthu.scopelab.tsqr.math.QRFactorMultiply;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import com.google.common.base.Optional;

public class QRJob{

 private JavaSparkContext sc;
 private JavaPairRDD<Long,sLMatrixWritable> matrdd;
 private JavaPairRDD<Long,sLMatrixWritable> Qrdd;
 private cmDenseMatrix R;
 private static boolean debug = true;
 private String Qspath = "temp/Qspath";
 
 public QRJob(JavaSparkContext sc,JavaPairRDD<Long,sLMatrixWritable> matrdd)
 {
  this.sc = sc;
  this.matrdd = matrdd;
  Qrdd = null;
  R = null;
 }
 
 public void run(int[] redSche) throws IOException
 {
  int itrNum = redSche.length;
  /* QRJob
	 *(1) merge A matrix and do QR decomposition get Qf (split to sub Qf depend on sub A) <rId,<uId,Qf>>, Rf <rId,<rsymbol,Rf>>
	 *(2) merge Rf, do QR factorization for Rf and get Rfm = Qsm x finalR, split Qsm to Qs (In master node)
	 *(3) join Qf <rId,<uId,Qf>> and Qs <rId,Qs> and do matrix multiplication for Qf * Qs to get finalQ <uId,finalQ>
  */
  JavaPairRDD<Long,Tuple3<Long,Integer,sLMatrixWritable>> QRrdd = matrdd.mapPartitions(new FirstQR());
  
  JavaPairRDD<Long,Tuple2<Long,sLMatrixWritable>> Qf = QRrdd.map(new select("Q",true)).filter(new fNull());
  JavaPairRDD<Long,Tuple2<Long,sLMatrixWritable>> Rf = QRrdd.map(new select("R",true)).filter(new fNull());

  JavaPairRDD<Long,Tuple2<Long,sLMatrixWritable>> pRrdd = Rf;
  
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path(Qspath),true);
  for(int i=0;i<itrNum-1;i++)
  {
   //gradually reduce number of partitions which is argument of "repartition" function
   pRrdd = pRrdd.repartition(redSche[i]).mapPartitions(new mergedQR(i+1,Qspath)); 
  }
  
	List<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>> RrddList = pRrdd.collect();
	if(RrddList.size()>1){
	//merge Rf and then do QR factorization to get Qs and Rs
	int columns = RrddList.get(0)._2._2.matNumColumns();
	int rows = columns*RrddList.size();
	cmDenseMatrix mR = new cmDenseMatrix(rows,columns);
	Long[] keyIdArray = new Long[RrddList.size()];	
	
	long mergedId;
	int count = 0;
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
	mergedId = keyIdArray[0];
	QRF qrf2 = QRF.factorize(mR);
    cmDenseMatrix Qsmat = qrf2.getQ();
	cmDenseMatrix finalR = qrf2.getR();
	this.R = finalR;
	//split Qs and write the last Qs file 	
	SequenceFile.Writer swriter = new SequenceFile.Writer(fs,conf,new Path(Qspath+"/"+itrNum+"/"+mergedId),LongWritable.class,sMatrixWritable.class);
	LongWritable okey = new LongWritable();
	sMatrixWritable ovalue = new sMatrixWritable();
	
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
	 swriter.append(okey,ovalue);
	}
	swriter.close();
	}// if end: RrddList.size > 1
	
	//Build Qs Map
	// (uId: Long,Tuple2<mergedId: Long, offset: Long>)
	List<Map<Long,Tuple2<Long,Long>>> QsMapList = new ArrayList<Map<Long,Tuple2<Long,Long>>>();

	LongWritable ikey = new LongWritable();
	for(int i=0;i<itrNum;i++)
	{
	Path QsdPath = new Path(Qspath+"/"+String.valueOf(i+1)+"/");
	FileStatus[] fstat = fs.listStatus(QsdPath);
	long offset;
	Map<Long,Tuple2<Long,Long>> QsMap = new HashMap<Long,Tuple2<Long,Long>>();
	for(int j=0;j<fstat.length;j++)
	{
	 long mergedId = Long.valueOf(fstat[j].getPath().getName());
	 SequenceFile.Reader sreader = new SequenceFile.Reader(fs,fstat[j].getPath(),conf);
	 String filename = fstat[j].getPath().toString();
	 offset=sreader.getPosition();
	 
	 while(sreader.next(ikey))
	 {
	  //System.out.println(i+": QsMap put "+ikey.get()+","+mergedId+","+offset);
	  QsMap.put(ikey.get(),new Tuple2<Long,Long>(mergedId,offset));
	  offset = sreader.getPosition();
	 }
	 sreader.close();
	}
	QsMapList.add(QsMap); //The order in QsMapList means that level of iteration
	}
	
	//sequentially join Qf and Qs then compute Multiplication
	//Qf use mergedId for key and Qs use uId for key
	
	JavaPairRDD<Long,sLMatrixWritable> Qmat = Qf.mapPartitions(new buildQ(sc.broadcast(QsMapList),sc.broadcast(Qspath)));
	Qrdd = Qmat;
	
	//Using "QR - A ~= zero matrix" to verify our QR decomposition is correct
	List<Double> doubleList =  Qrdd.join(matrdd).map(new QRsubA(this.R)).collect();
	int count = 0;
 }
  
  public JavaPairRDD<Long,sLMatrixWritable> getQrdd()
  {
   return Qrdd;
  }
  
  public cmDenseMatrix getR()
  {
   return R;
  }
   
  /*
   * Input: Iterator<<uIdi,Amat>>
   * Output: <mergedIdo,<uIdo,Qmark,Qf>> or <mergedIdo,<mergedIdo,Rmark,Rf>> (uIdo = uIdi and mergedIdo is unique for single partition.)
   * merged A matrix, and do QR decomposition, output Rf and Qf
   */
  public static class FirstQR extends PairFlatMapFunction<Iterator<Tuple2<Long,sLMatrixWritable>>, Long, Tuple3<Long,Integer,sLMatrixWritable>>
 {
    private long mergedId=-1; //cannot get task Id in this function so we assign a unique Id to "mergedId" for substitute
	private long t1, t2, computationTime=0;
	
  public Iterable<Tuple2<Long, Tuple3<Long,Integer,sLMatrixWritable>>> call(Iterator<Tuple2<Long,sLMatrixWritable>> iter) {
	t1 = new Date().getTime();
	List<Tuple2<Long,sLMatrixWritable>> AsList = new ArrayList<Tuple2<Long,sLMatrixWritable>>();
	int Amrows = 0, Amcols = 0;
	while(iter.hasNext())
	{	 
	 Tuple2<Long,sLMatrixWritable> value = iter.next();
	 if(mergedId==-1)
		mergedId = value._1;
	 AsList.add(value);	 
	 Amrows+=value._2.getDense().numRows(); //accumulate all row size of As
     Amcols = value._2.getDense().numColumns();	 
	}//end while
	
	int i,j,k;
	//merged
   cmDenseMatrix Am = new cmDenseMatrix(Amrows,Amcols);
   int accRows = 0;
   for(i=0;i<AsList.size();i++)
   {
    cmDenseMatrix As = AsList.get(i)._2.getDense();
    int Arows = As.numRows();	
	for(j=0;j<Arows;j++)
	{
	 for(k=0;k<Amcols;k++)
	  Am.set(accRows+j,k,As.get(j,k));
	}
	accRows+=Arows;
   }
      
   //QR decomposition
   QRF qrf = QRF.factorize(Am);
   cmDenseMatrix Q = qrf.getQ();
   cmDenseMatrix R = qrf.getR();
   
   List<Tuple2<Long, Tuple3<Long,Integer,sLMatrixWritable>>> outputList = new ArrayList<Tuple2<Long, Tuple3<Long,Integer,sLMatrixWritable>>>();
   //split Q
   accRows = 0;
   for(i=0;i<AsList.size();i++)
   {
	int Arows = AsList.get(i)._2.getDense().numRows();
	cmDenseMatrix Qf = new cmDenseMatrix(Arows,Q.numColumns());
        for(j=0;j<Arows;j++)
        {
         for(k=0;k<Amcols;k++)
			Qf.set(j,k,Q.get(accRows+j,k));
        }
	accRows+=Arows;
	outputList.add(new Tuple2<Long, Tuple3<Long,Integer,sLMatrixWritable>>(mergedId, new Tuple3<Long,Integer,sLMatrixWritable>(AsList.get(i)._1,0,new sLMatrixWritable(AsList.get(i)._2.getLongArray(),Qf))));
   }
   //add R
   outputList.add(new Tuple2<Long, Tuple3<Long,Integer,sLMatrixWritable>>(mergedId, new Tuple3<Long,Integer,sLMatrixWritable>(mergedId,1,new sLMatrixWritable(R))));
   t2 = new Date().getTime();
   computationTime+=(t2-t1);
   System.out.println(mergedId+", computationTime: "+computationTime);
   
   return outputList; 
   }// call
  }
  
  /* 
   * Input: <mergedIdi,<uIdi,isQ or isR,Q or R>>
   * Output 
   * selectQ: <mergedIdi,<uIdi,Q>> or selectR: <mergedIdi,<uIdi,R>> (mergedIdi and uIdi of R matrix that have same value.)
  */
  public static class select extends PairFunction<Tuple2<Long,Tuple3<Long,Integer,sLMatrixWritable>>, Long, Tuple2<Long,sLMatrixWritable>>
  {
   boolean isQ, firstSelect;
   
   public select(String QorR, boolean firstSelect)
   {
    if(QorR.toLowerCase().equals("q"))
	 isQ = true;
	else
	 isQ = false;
	 this.firstSelect = firstSelect;
   }
   
   public Tuple2<Long, Tuple2<Long,sLMatrixWritable>> call(Tuple2<Long,Tuple3<Long,Integer,sLMatrixWritable>> kvpair) {   
	 if(isQ && kvpair._2._2()==0) //select Q
	  return new Tuple2<Long, Tuple2<Long,sLMatrixWritable>>(kvpair._1,new Tuple2<Long,sLMatrixWritable>(kvpair._2._1(),kvpair._2._3()));
	 else if(!isQ && kvpair._2._2()==1) //select R
	  return new Tuple2<Long, Tuple2<Long,sLMatrixWritable>>(kvpair._1,new Tuple2<Long,sLMatrixWritable>(kvpair._2._1(),kvpair._2._3()));
	 else
	  return null;
   }
  }
  
  //use groupByKey to produce R_RDD before pass this function in flatMap
  //Input: Iterator<(mergedIdi,<uIdi,R>)> (mergedIdi and uIdi of R matrix that have same value.)
  //Output: <uIdo,<mergedIdo,R>> ( uIdo=uIdi and mergedIdo is unique for single partition.)
  public static class mergedQR extends PairFlatMapFunction<Iterator<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>>, Long, Tuple2<Long,sLMatrixWritable> >{
		
	private long mergedId;
	private int itorder;
	private String Qspath;
	private long tt1, tt2, writingTime=0;

	public mergedQR(int itorder,String Qspath)
	{
	 this.itorder = itorder;
	 this.Qspath = Qspath;
	 this.mergedId = -1;
	}
	
	public Iterable<Tuple2<Long, Tuple2<Long,sLMatrixWritable>>> call(Iterator<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>> iter) throws IOException
	{
		
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	
	List<Tuple2<Long, Tuple2<Long,sLMatrixWritable>>> outputList = new ArrayList<Tuple2<Long, Tuple2<Long,sLMatrixWritable>>>();
	List<Tuple2<Long,sLMatrixWritable>> RsList = new ArrayList<Tuple2<Long,sLMatrixWritable>>();
		
	while(iter.hasNext())
	{
		Tuple2<Long,Tuple2<Long,sLMatrixWritable>> ivalue = iter.next();
		if(mergedId==-1)
			mergedId = ivalue._1;
		RsList.add(ivalue._2);
	}
	
	if(RsList.size()>0)
	{
	
	int columns = RsList.get(0)._2.matNumColumns();
	int rows = columns*RsList.size();	
	
	cmDenseMatrix mR = new cmDenseMatrix(rows,columns);
	Long[] keyIdArray = new Long[RsList.size()];
	int count = 0;
	for(Tuple2<Long,sLMatrixWritable> Rkvpair: RsList)
	{
	 keyIdArray[count] = Rkvpair._1;
	 cmDenseMatrix Rv = Rkvpair._2.getDense();
	 //R matrix shape is square, its row size equal to column size
	 for(int i=0;i<columns;i++)
	 for(int j=0;j<columns;j++)
	 {
	  mR.set(i+count*columns,j,Rv.get(i,j));
	 }
	 count++;
	}
	QRF qrf2 = QRF.factorize(mR);
    cmDenseMatrix Qsmat = qrf2.getQ();
	cmDenseMatrix Rs = qrf2.getR();
	outputList.add(new Tuple2<Long, Tuple2<Long,sLMatrixWritable>>(mergedId, new Tuple2<Long,sLMatrixWritable>(mergedId,new sLMatrixWritable(Rs))));
	
	//split Qs and wrtie it to HDFS
	SequenceFile.Writer swriter = new SequenceFile.Writer(fs,conf,new Path(Qspath+"/"+itorder+"/"+mergedId),LongWritable.class,sMatrixWritable.class);
	LongWritable okey = new LongWritable();
	sMatrixWritable ovalue = new sMatrixWritable();
	for(int i=0;i<keyIdArray.length;i++)
	{
	 cmDenseMatrix Qsvalue = new cmDenseMatrix(columns,columns);
	 for(int j=0;j<columns;j++)
	 for(int k=0;k<columns;k++)
	 {
	  Qsvalue.set(j,k,Qsmat.get(j+columns*i,k));
	 }
		
	okey.set(keyIdArray[i]);
	ovalue.set(Qsvalue);
	swriter.append(okey,ovalue);
	}
	swriter.close();	
	}//end if: RsList.size() > 0
	return outputList;
    }
  }
    
  //Join Q and A and compute QR - A
  public static class QRsubA extends DoubleFunction<Tuple2<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>>>
  {
	private cmDenseMatrix R;
	
	public QRsubA(cmDenseMatrix R)
	{
		this.R = R;
	}
	
   public Double call(Tuple2<Long,Tuple2<sLMatrixWritable,sLMatrixWritable>> kvpair) {
	cmDenseMatrix Q = kvpair._2._1.getDense();
	cmDenseMatrix A = kvpair._2._2.getDense();
	//Multiply
	cmDenseMatrix QR = new cmDenseMatrix(Q.numRows(),R.numColumns());
	QRFactorMultiply.Multiply("N","N",Q,R,QR);
	
	double acc = 0;
	//Compute QR - A and accumulate all elements
	for(int i=0;i<A.numRows();i++)
	 for(int j=0;j<A.numColumns();j++)
	  acc += Math.abs(Math.abs(A.get(i,j)) - Math.abs(QR.get(i,j)));
	
	acc = acc/(A.numRows()*A.numColumns());
	
	return acc;
   }
  }
  
  public static class buildQ extends PairFlatMapFunction<Iterator<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>>, Long, sLMatrixWritable>
  {
	private long t1, t2;
	private int rndId, computationTime;
	private String Qspath;
	private List<Map<Long,Tuple2<Long,Long>>> QsMapList;
	
	public buildQ(Broadcast<List<Map<Long,Tuple2<Long,Long>>>> broadcastVar, Broadcast<String> bcQspath)
	{
		QsMapList = broadcastVar.value();
		Qspath = bcQspath.value();
		rndId = -1;
		computationTime = 0;
	}
	
   public Iterable<Tuple2<Long,sLMatrixWritable>> call(Iterator<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>> iter) throws IOException
   {
    
    t1 = new Date().getTime();
	List<Tuple2<Long,sLMatrixWritable>> outputList = new ArrayList<Tuple2<Long,sLMatrixWritable>>();
	long testId = -1;
    Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	LongWritable ikey = new LongWritable();
	sMatrixWritable invalue = new sMatrixWritable();	
	cmDenseMatrix tempQ;
	
    while(iter.hasNext())
	{
		Tuple2<Long,Tuple2<Long,sLMatrixWritable>> itvalue = iter.next();
		cmDenseMatrix Qf = itvalue._2._2.getDense();
		
		long mergedId = itvalue._1;
		testId = mergedId;
		List<cmDenseMatrix> buildQList = new ArrayList<cmDenseMatrix>();
		for(int i=0;i<QsMapList.size();i++)
		{
		 Tuple2<Long,Long> mapvalue = QsMapList.get(i).get(mergedId); //get (Qs's mergedId,ofsset) by matching Qf's mergedId and Qs's uId
		 SequenceFile.Reader sreader = new SequenceFile.Reader(fs,new Path(Qspath+"/"+String.valueOf(i+1)+"/"+String.valueOf(mapvalue._1)),conf);
		 sreader.seek(mapvalue._2);
		 sreader.next(ikey,invalue);
		 buildQList.add(invalue.getDense().copy());
		 mergedId = mapvalue._1;//ikey.get();
		 sreader.close();
		}
		cmDenseMatrix Q = new cmDenseMatrix(Qf.numRows(),buildQList.get(0).numColumns());
		for(int i=0;i<buildQList.size();i++)
		{
		 //System.out.println("Qs: "+i);
		 //System.out.println(buildQList.get(i));
		 
		 QRFactorMultiply.Multiply("N","N",Qf,buildQList.get(i),Q);
		 //Qf and Q matrix have same size
		 tempQ = Qf;
		 Qf = Q;
		 Q = tempQ;
		}
		Q = Qf;
		itvalue._2._2.set(Q);
		outputList.add(new Tuple2<Long,sLMatrixWritable>(itvalue._2._1,itvalue._2._2));
	}
    if(rndId==-1)//For debug
	{
		Random rnd = new Random();
		rndId = Math.abs(rnd.nextInt());
	}
	//return Result
	t2 = new Date().getTime();
	computationTime+=(t2-t1);
	//if(debug)
	System.out.println(rndId+": BuildQ ComputeTime: "+computationTime);
	return outputList;
   }
  }
  
  //remove the mergedId of last sub-matrix Q
  //Input: <mergedId_s,<uId1,Q1>
  //Output: <uId1,Q> 
  public static class QLastJob extends PairFunction<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>, Long, sLMatrixWritable>
  {
	public Tuple2<Long,sLMatrixWritable> call(Tuple2<Long,Tuple2<Long,sLMatrixWritable>> kvpair) {
		return new Tuple2<Long,sLMatrixWritable>(kvpair._2._1,kvpair._2._2);
	}
  }
  
  public static class fNull extends Function<Tuple2<Long,Tuple2<Long,sLMatrixWritable>>, Boolean>
  {
	public Boolean call(Tuple2<Long,Tuple2<Long,sLMatrixWritable>> kvpair) { return kvpair!=null; }
  }
}
