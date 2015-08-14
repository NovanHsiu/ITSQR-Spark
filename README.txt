ITSQR: Iterative Tall-and-Skinny QR Factorization in Spark Implementation
Version 0.1
Distributed in 2014/09/04
===

### Hsiu-Cheng Yu, Che-Rung Lee


# Introduction
----------------
The function provided by this package is a QR factorizations for Tall-and-Skinny matrices (TSQR) implementation.  
A 'Tall-and-Skinny matrix' which number of rows is much bigger than its number of columns. 
The TSQR function can be used to solve many problems, such as stochastic SVD (SSVD). You can find more detail about 
this implemenation in my thesis "Implemenations of TSQR for Cloud Platforms and Its Applications of SSVD and Collaborative Filtering".

In this implementation, we provide three functions:

 * TSQR: Compute Q and R matrices of given tall-and-skinny matrix.
 * Recommendation System: An application of stochastic SVD, Use item-based collaborative filtering to implement a recommendation system
 
This code is written in Java and uses JLAPACK and JBLAS library for matrix computation.

# Environment Setup
--------
This code is for Linux systems.  This implementation is based on several library. Make sure those library are properly installed before running this program.

 * Apache Hadoop 1.x version (http://hadoop.apache.org/)
 * Apache Maven (http://maven.apache.org/)
 * Apache Spark 0.9.x version (http://spark.apache.org/)
   This implementation is running on Spark standalone mode
 * matrix-toolkits-java (https://github.com/fommil/matrix-toolkits-java)
 * JLAPACK, JBLAS and F2JUTLI (http://www.netlib.org/java/f2j/)
 * ITSQR package in Hadoop MapReduce implementation ()
 
# Compiling 
--------------
First, decompress our archive into installed directory of Spark. And setup following paths in script "compute-classpath.sh" and "pom.xml".
There are five paths of libraries need to setup in "compute-classpath.sh": MTJ, JLAPACK, JBLAS, JUTIL and HITSQR.

 - MTJ 
  Path where matrix-toolkits-java is installed.
 - JLAPACK
  Path where is lapack.jar package
 - JBLAS
  Path where is blas.jar package
 - JUTIL
  Path where is f2jutil.jar package
 - HITSQR
  Path of TSQR.jar, the package of ITSQR in Hadoop implementation
  
Using Maven to compile and archive, or remove the old file.
 (1) $ mvn install
     Compile and archive all Class files
 (2) $ mvn clean
     Remove archive file and directory of Class file
	 
# Usage Example and Command Line Arguments
----------------------------------------------------

 $(MTJ)
  Path of matrix-toolkits-java package
 $(JLAPACK)
  Path where is lapack.jar package
 $(JBLAS)
  Path where is blas.jar package
 $(JUTIL)
  Path where is f2jutil.jar package
 $(SPARK_HOME)
  Path where Spark is installed
 $(TSQR_DIR)
  Path where TSQR package is installed
  TSQR_DIR=$(SPARK_HOME)/ITSQR_Spark_v1
  
## Upload our example for experiment of ITSQR or SSVD
 $ hadoop fs -mkdir tsqr
 $ hadoop fs -copyFromLocal testdata/100x5
 
In the input file of example, one line means one row in a matrix, and numbers are separated by a space. 
If you want to use other matrices for this code, the format of matrices must follow above rules.

## Upload our example for experiment of Recommendation System
 $ hadoop fs -mkdir ratingData
 $ hadoop fs -copyFromLocal testdata/testRatingData
 
 The input of recommendation system is composed by user ratings for items in text file, looks like following example:
 <user id> <item id> <rating value>
 108 123 3.5

## Example: Run Iterative TSQR
### Do QR Factorization only
 ${SPARK_HOME}/ITSQR_Spark_v1/run-tsqr nthu.scopelab.stsqr.TSQR \
 -master spark://scopion-headnode:7066 \
 -input input/mat30x10 \
 -output outputQ \
 -subRowSize 10 \
 -em 2g

#### Explanation of Arguments for Running Iterative TSQR
The argument has star mark * means that it must be given a value by user and other arguments without star mark have default value.
 
 -master *
  location of dameon process on master node
 -input *
  Input file, including its directory.
 -output *
  Output directory
 -subRowSize * (default: equal to number of columns)
  It is the number of rows of each submatrix. These submatrices are split from input matrix. subRowSize must be bigger than the number of columns, and smaller than the number of rows of the original matrix.
 -em (default: 512m)
  Memory for each executor on slave node
 -reduceSchedule (default: 1) 
  This argument is used in QRFirstJob. The input can be a sequence of numbers, which are separated by commas.  The ith number in the sequence represents the number of Reducers in the ith MapReduce task.
  Finally, last number need to set to one in order to calculate the final R.
  For example, 4,1 means that it has two MapReduce and first MapReduce has four Reducers and second has one reducer.
 
## Example: Running Iterative TSQR SSVD (ITSSVD) Item-Based Recommendation System 
 ${SPARK_HOME}/ITSQR_Spark_v1/run-tsqr nthu.scopelab.stsqr.ssvd.rec.RecommendationRunner \
 -master spark://scopion-headnode:7066 \
 -input ratingData/testRatingData \
 -output outputRS \
 -rank 10 \
 -oversampling 50 \
 -subRowSize 1000 \
 -numrec 10 \
 -em 2g

#### Explanation of Arguments for Running Recommendation System 
 - input, output, ....
  The same as they are in the TSQR example
 - rank *
  "rank" is Stochastic number that would simply m*n matrix to m*k matrix. (k is smaller than n)
 - oversampling
  "oversampling" is a sampling value for reducing deviation of SSVD and this value would increase the size of column in calculation
 - numrec
  Number of recommended item for per user
   
# Tuning Suggestion of Arguments
--------------------------------
If you want improve performance for ITSQR by tuning argument. We have some tips for following three arguments.

-subRowSize
This argument does not has obvious influence for performance of application in this package, but it has a point need to know. 
If the argument is too large which cause the data size of sub matrix is bigger than Map task input split size, 
that will dramatically reduce the performance because of unblance workload for each task.

-reduceSchedule
The default value of this argument was one (1). If you encounter the out of memory problem of JVM or performance bottleneck in QRFirstJob or QJob
that caused by bigger data, you could find more detail in section 5.4.2 of my thesis which is descirbed in above part of introduction.

# Overview
--------
 * src/main/java/nthu/scopelab/stsqr/TSQR.java - driver code for TSQR
 * src/main/java/nthu/scopelab/stsqr/ssvd/rec/RecommendationRunner.java - driver code for Recommendation System
 * src/main/java/nthu/scopelab/stsqr/ssvd/rec/RecSplitVRunner.java - driver code for Recommendation System with another method of implementation
 
# References
--------------
 * Direct QR factorizations for tall-and-skinny matrices in MapReduce architectures [[pdf](http://arxiv.org/abs/1301.1071)]
 * Tall and skinny QR factorizations in MapReduce architectures [[pdf](http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf)]
 * [MAHOUT-376] Implement Map-reduce version of stochastic SVD [https://issues.apache.org/jira/browse/MAHOUT-376]

Contact
--------
 * For any questions, suggestions, and bug reports, email Hsiu-Cheng Yu by s411051@gmail.com please.
 * This code can be reached at: https://github.com/NovanHsiu/ITSQR-Spark
 
