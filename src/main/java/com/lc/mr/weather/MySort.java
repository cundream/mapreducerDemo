package com.lc.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator{
	
	public MySort() {
		super(MyKey.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey key =(MyKey)a;
		MyKey key2 =(MyKey)b;
		int year = Integer.compare(key.getYear(), key2.getYear());
		if(year == 0){
			int month = Integer.compare(key.getMonth(), key2.getMonth());
			if(month == 0){
				return -Double.compare(key.getHot(), key2.getHot());
			}else{
				return month;
			}
		}else{
			return year;
		}
	}
}
