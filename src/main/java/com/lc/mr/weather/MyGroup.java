package com.lc.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{
	
	public MyGroup() {
		super(MyKey.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey key =(MyKey)a;
		MyKey key2 =(MyKey)b;
		int year = Integer.compare(key.getYear(), key2.getYear());
		if(year == 0){
			return  Integer.compare(key.getMonth(), key2.getMonth());
		}else{
			return year;
		}
	}
}
