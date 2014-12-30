package nthu.scopelab.stsqr.ssvd.rec;

import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

public class TopkItem implements Serializable{
	private int numrec;
	private int curnumitem;
	private Tuple2<Long,Double>[] item;
	private List<Tuple2<Long,Double>> itemlist;
	
	public TopkItem(int numrec)
	{
	 this.numrec = numrec;
	 this.itemlist = new ArrayList<Tuple2<Long,Double>>();
	 this.curnumitem = 0;
	}
	
	public TopkItem(int numrec,int numitem)
	{
	 this.numrec = numrec;
	 this.itemlist = new ArrayList<Tuple2<Long,Double>>();
	 this.curnumitem = numitem;
	}
	
	public void offer(Tuple2<Long,Double> item)
	{
	 
	 
	 if(curnumitem<numrec)
	 {
	  itemlist.add(item);
	  curnumitem++;
	  //if the new item bigger than any item in the list then shift the itemlist and insert the new item, else don't do anything.
	  for(int i=0;i<curnumitem;i++) 
	  {
	  if(itemlist.get(i)._2<item._2)
	  {
	    for(int j=curnumitem-1;j>i;j--)
		 itemlist.set(j,itemlist.get(j-1));		 
	    itemlist.set(i,item);
		break;
	  }
	  }	 	  
	  
	 }
	 else
	 {
	  
	  for(int i=0;i<numrec;i++)
	  {
	   if(itemlist.get(i)._2<item._2)
	   {
	   
	    for(int j=numrec-1;j>i;j--)
		 itemlist.set(j,itemlist.get(j-1));
		 
	    itemlist.set(i,item);
		break;
	   }
	  }
	 }
	}
	
	public int numItem()
	{
	 return curnumitem;
	}
	
	public int numRecommendation()
	{
	 return numrec;
	}
	
	public void set(int i,Tuple2<Long,Double> item)
	{
	 itemlist.set(i,item);
	}
	
	public Tuple2<Long,Double> get(int i)
	{
	 return itemlist.get(i);
	}
	
	public List<Tuple2<Long,Double>> getList()
	{
	 return itemlist;
	}
	
	@Override
	public String toString()
	{
	 String text = "";
	 for(int i=0;i<curnumitem;i++)
	  text = text + itemlist.get(i)._1.toString()+":" + itemlist.get(i)._2.toString()+",";
	 return text;
	}
}