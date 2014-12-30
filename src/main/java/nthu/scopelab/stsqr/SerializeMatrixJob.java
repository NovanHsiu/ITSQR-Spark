package nthu.scopelab.stsqr;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.TextInputFormat;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class SerializeMatrixJob{

 private JavaSparkContext sc;
 private int rbs;
 private String path;
 
 public SerializeMatrixJob(JavaSparkContext sc,int rbs, String path)
 {
  this.sc = sc;
  this.rbs = rbs;
  this.path = path;
 }
 
 public JavaPairRDD<Long,sLMatrixWritable> run()
 {
  //use readHadoop function read text file, split and merge string row vector depend on rbs
  JavaPairRDD<Long,sLMatrixWritable> subMatrix = sc.hadoopFile(path,TextInputFormat.class,LongWritable.class,Text.class).mapPartitions(new mergeRows(rbs));
  
  return subMatrix;
 }
 
 private static class makeRows extends PairFunction<Tuple2<LongWritable,Text>, Long, String>
 {

  private long leaderOffset;
  
  public makeRows()
  {
   this.leaderOffset = -1;
  }
  
  public Tuple2<Long,String> call(Tuple2<LongWritable,Text> kvpair) {
   //need to set this property in BackEndExecutor
   //int workerId = Integer.valueOf(System.getProperty("executor.Id"));
   String offset = Long.toString(kvpair._1.get());
   String ovalue = offset + " " + kvpair._2.toString();
   
   if(leaderOffset==-1)
    leaderOffset = Long.valueOf(offset);
   
   return new Tuple2<Long,String>(leaderOffset,ovalue);
  }
 }
 
 private static class mergeRows extends PairFlatMapFunction<Iterator<Tuple2<LongWritable,Text>>, Long, sLMatrixWritable>
 {
  private int cols = -1;
  private int rbs;
  
  public mergeRows(int rbs)
  {
   this.rbs = rbs;
  }
  
  public Iterable<Tuple2<Long, sLMatrixWritable>> call(Iterator<Tuple2<LongWritable,Text>> iter) {
   
	List<String> curRowBuffer = new ArrayList<String>();
	List<Long> userIdList = new ArrayList<Long>();
	List<Tuple2<Long, sLMatrixWritable>> outputList = new ArrayList<Tuple2<Long, sLMatrixWritable>>();
	
	while(iter.hasNext())
	{
		Tuple2<LongWritable, Text> ivalue = iter.next();
		if(cols==-1)
			cols = ivalue._2.toString().split(" ").length - 1;
		
		curRowBuffer.add(ivalue._2.toString());
		userIdList.add(ivalue._1.get());
		
		if(curRowBuffer.size()>=rbs*2) //Avoid the column size of final merged sub-matrix larger than row size. 
		{
			cmDenseMatrix mat = new cmDenseMatrix(rbs,cols);
			long[] larray = new long[rbs];
			for(int i=0;i<rbs;i++)
			{	
				larray[i] = userIdList.remove(0);
				String[] rowstr = curRowBuffer.remove(0).split(" ");
				for(int j=0;j<cols;j++)
				{
					mat.set(i,j,Double.valueOf(rowstr[j]));
				}
			}
			outputList.add(new Tuple2<Long,sLMatrixWritable>(larray[0],new sLMatrixWritable(larray,mat)));
		}
	}
	
	int rrows = curRowBuffer.size();
	if(rrows>0) 
	{
		cmDenseMatrix mat = new cmDenseMatrix(rrows,cols);
		long[] larray = new long[rrows];
		for(int i=0;i<rrows;i++)
		{	
			larray[i] = userIdList.remove(0);
			String[] rowstr = curRowBuffer.remove(0).split(" ");
			for(int j=0;j<cols;j++)
			{
					mat.set(i,j,Double.valueOf(rowstr[j]));
			}
		}
		outputList.add(new Tuple2<Long,sLMatrixWritable>(larray[0],new sLMatrixWritable(larray,mat)));
	}
		return outputList;
	}
 }
 
}