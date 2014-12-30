package nthu.scopelab.stsqr.ssvd.rec;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.SparseVector;

import nthu.scopelab.tsqr.matrix.sLMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmSparseMatrix;
import nthu.scopelab.stsqr.TSQR;

import nthu.scopelab.stsqr.ssvd.SSVDRunner;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class RecSplitVRunner{

	public static void main(String[] args) throws Exception {
	 //getArgument
	 String projectdir = TSQR.getArgument("-projectDir",args);
	if (projectdir == null) {
            System.out.println("Required argument '-projectDir' missing");
            return;
    } 
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
	
	String thItem_str = TSQR.getArgument("-thItem",args);
	if(thItem_str ==null)
	 thItem_str = "1";
	
	String numrec_str = TSQR.getArgument("-numrec",args);
	if(numrec_str==null)
	{
	 numrec_str = "10";
	}
	
	String srsv_str = TSQR.getArgument("-subRowSizeV",args);
	if (srsv_str == null) {
        srsv_str = "100";
    }
	
	String sv_str = TSQR.getArgument("-splitV",args);
	if (sv_str == null) {
        sv_str = "2";
    }
	
	String hf_str = TSQR.getArgument("-hasprefile",args);
	if(hf_str == null ){
	 hf_str = "false";
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
	int thItem = Integer.valueOf(thItem_str);
	int numrec = Integer.valueOf(numrec_str);
	int splitv = Integer.valueOf(sv_str);
	boolean hasprefile = Boolean.parseBoolean(hf_str);
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
	SparkConf sconf;
	JavaSparkContext ctx;
	long start=0, start2=0, end=0;

	if(!hasprefile)
	{
	sconf = new SparkConf().setMaster(master)
	.setAppName("PrepartionAndSSVDJob")
	.setSparkHome(sparkHome)
	.setJars(new String[]{projectdir+"/target/simple-project-1.0.jar"})
	.set("spark.executor.memory", exememory);
    ctx = new JavaSparkContext(sconf);
	Thread.sleep(2000);
	start = new Date().getTime();
	//1. PreparationJob
	ToUserMatrixJob matrixrdd = new ToUserMatrixJob(ctx,new Integer(subRowSize),inputpath);	
	JavaPairRDD<Long,sLMatrixWritable> Ardd = matrixrdd.run();
	Ardd.setName("Ardd");
	Ardd = Ardd.cache();
	//2. SSVD
    SSVDRunner ssvdrunner = new SSVDRunner(Ardd,ctx,outputpath,k,p,r,h,abh,q,subRowSizeV,rs_str,computeU,cUHalfSigma,computeV,cVHalfSigma,overwrite);	 
	ssvdrunner.run();
	end = new Date().getTime();
	//3. Recommend: renew a SparkContext
	ctx.stop();
	start2 = new Date().getTime();
	sconf = new SparkConf().setMaster(master)
	.setAppName("RecommendedJob")
	.setSparkHome(sparkHome)
	.setJars(new String[]{projectdir+"/target/simple-project-1.0.jar"})
	.set("spark.executor.memory", exememory);	
	ctx = new JavaSparkContext(sconf);
	RecSplitVJob recjob = new RecSplitVJob(ctx,ssvdrunner.getUPath(),outputpath+"/Recommendation",ssvdrunner.getVPath(), ssvdrunner.getSigma(), thItem, numrec,splitv);
	recjob.run();
	}//fi: !hasfile
	else
	{	 
	start2 = new Date().getTime();
	sconf = new SparkConf().setMaster(master)
	.setAppName("RecommendedJob")
	.setSparkHome(sparkHome)
	.setJars(new String[]{projectdir+"/target/simple-project-1.0.jar"})
	.set("spark.executor.memory", exememory);	
	ctx = new JavaSparkContext(sconf);
	RecSplitVJob recjob = new RecSplitVJob(ctx,outputpath+"/U",outputpath+"/Recommendation",outputpath+"/V", outputpath+"/Sigma/svalues.seq", thItem, numrec,splitv);
	recjob.run();
	}

	end = new Date().getTime();
	System.out.println("Finished in: "+(end-start));
	System.exit(0);
	}

}
