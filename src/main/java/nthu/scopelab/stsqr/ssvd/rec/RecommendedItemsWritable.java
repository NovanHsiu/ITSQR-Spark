package nthu.scopelab.stsqr.ssvd.rec;

import org.apache.hadoop.io.Writable;
import scala.Tuple2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class RecommendedItemsWritable implements Writable, Serializable {

    private TopkItem topkitem;
	private int numrec;
	private int numitem;
	
	public RecommendedItemsWritable(TopkItem topkitem) {
		this.topkitem = topkitem;
		this.numrec = topkitem.numRecommendation();
		this.numitem = topkitem.numItem();
    }
	
	@Override
    public void write(DataOutput out) throws IOException {
		out.writeInt(numitem);
		out.writeInt(numrec);
		for(int i=0;i<numitem;i++)
		{
		 out.writeLong(topkitem.get(i)._1);
		 out.writeDouble(topkitem.get(i)._2);
		}
    }
	
	@Override
    public void readFields(DataInput in) throws IOException {
		numitem = in.readInt();
		numrec = in.readInt();
		
		topkitem = new TopkItem(numrec,numitem);
		for(int i=0;i<numitem;i++)
		 topkitem.set(i,new Tuple2<Long,Double>(in.readLong(),in.readDouble()));
    }
	
	public void set(TopkItem topkitem) {
		this.topkitem = topkitem;
		this.numrec = topkitem.numRecommendation();
		this.numitem = topkitem.numItem();
    }
		
    public TopkItem get() {
		return topkitem;
    }   
	
	@Override
	public String toString() {
		return "["+topkitem.toString()+"]";
	}
}
