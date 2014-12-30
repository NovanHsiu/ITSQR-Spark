package nthu.scopelab.stsqr.ssvd.rec;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.TextInputFormat;

import no.uib.cipr.matrix.sparse.SparseVector;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmSparseMatrix;

import java.util.List;
import java.util.ArrayList;

public class ToUserMatrixJob{

 private JavaSparkContext sc;
 private int rbs;
 private String path;
 
 public ToUserMatrixJob(JavaSparkContext sc,int rbs, String path)
 {
  this.sc = sc;
  this.rbs = rbs;
  this.path = path;
 }
 
 public JavaPairRDD<Long,sLMatrixWritable> run()
 {
  //first step: split the text file and group these item by user(key), 
  JavaPairRDD<Long,List<String>> splitrdd = sc.textFile(path).map(new SplitUserItem()).groupByKey(); //repartition(120)
  JavaPairRDD<Long,String> rowrdd = splitrdd.map(new makeRows());
  //makerRows
  
  //split and merge string row vector depend on rbs
  JavaPairRDD<Long,sLMatrixWritable> subMatrix = rowrdd.groupByKey().flatMap(new mergeRows(rbs)); 
  //Can do an experiment about omitting combination part to test the performance
   
  return subMatrix;
 }
 
  private static class SplitUserItem extends PairFunction<String, Long, String>
 {
  private String regx = "[^0-9^a-z^A-Z^.]";
  public Tuple2<Long,String> call(String line) {
   String[] lineSplit = line.split(regx);
   Long key = Long.valueOf(lineSplit[0]);
   String value = lineSplit[1]+","+lineSplit[2];
   
   return new Tuple2<Long,String>(key,value);
  }
 }
 
 private static class makeRows extends PairFunction<Tuple2<Long,List<String>>, Long, String>
 {

  private long leaderUserID;
  
  public makeRows()
  {
   this.leaderUserID = -1;
  }
  
  public Tuple2<Long,String> call(Tuple2<Long,List<String>> kvpair) {

   String userId = Long.toString(kvpair._1);
   List<String> itemList = kvpair._2;
   String ovalue = userId;
   for(String itemAndRating : itemList)
   {
    ovalue = ovalue + " " + itemAndRating;
   }
   
   if(leaderUserID==-1)
    leaderUserID = Long.valueOf(userId);
   
   return new Tuple2<Long,String>(leaderUserID,ovalue);
  }
 }
 
 private static class mergeRows extends PairFlatMapFunction<Tuple2<Long,List<String>>, Long, sLMatrixWritable>
 {
  private int cols = -1;
  private int rbs;
  public mergeRows(int rbs)
  {
   this.rbs = rbs;
  }
  
  public Iterable<Tuple2<Long, sLMatrixWritable>> call(Tuple2<Long,List<String>> kvpair) {
   
   List<String> rowslist = kvpair._2;
   if(cols==-1)
    cols = rowslist.get(0).split(" ").length - 1;
   
   int remainRowSize = rowslist.size()%rbs;
   int numSplit = rowslist.size()/rbs;
   if(remainRowSize>0)
    numSplit+=1;
	
   int finalRowSize = remainRowSize;
   if(remainRowSize<cols && remainRowSize!=0)
   {
    if(numSplit==0)
	{
	 //throw new Exception(" Exception in 'mergeRows' function! Have too many tasks and input size of matrix row is too small! Please reduce the number of tasks of previous Rdd which use 'makeRows' function. ");
	}
    //merge the last submatrix to second last submatrix
    finalRowSize =+rbs;
	numSplit-=1;
   }
   
   List<Tuple2<Long, sLMatrixWritable>> outputList = new ArrayList<Tuple2<Long, sLMatrixWritable>>();
   int rows = rbs;
   int count = 0;
   for(int i=0;i<numSplit;i++)
   {
	if(i==numSplit-1 && remainRowSize>0)
	 rows = finalRowSize;
	
    long[] userIdarr = new long[rows];
	cmSparseMatrix mat = new cmSparseMatrix(rows,Integer.MAX_VALUE);
	for(int j=0;j<rows;j++)
	{
	 //System.out.println(rowslist.get(count));
	 String[] rowstr = rowslist.get(count).split(" ");
	 count++;
	 
	 SparseVector userVector = new SparseVector(Integer.MAX_VALUE);
	 userIdarr[j] = Long.valueOf(rowstr[0]);
	 
	 for(int k=1;k<rowstr.length;k++)
	 {
	  String[] item_rating = rowstr[k].split(",");
	  //Assume all item indexes are less than Integer.Max_VALUE
	  int index = Integer.valueOf(item_rating[0]);
	  double value = Double.valueOf(item_rating[1]);
	  userVector.set(index, value);
	 }
	 mat.setRow(j,userVector);
	}
	outputList.add(new Tuple2<Long,sLMatrixWritable>(userIdarr[0],new sLMatrixWritable(userIdarr,mat)));
   }
  
	return outputList;
  }
 }
 
}
