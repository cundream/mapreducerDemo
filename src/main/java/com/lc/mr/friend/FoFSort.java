package com.lc.mr.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FoFSort extends WritableComparator{

	public FoFSort() {
		super(User.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		User u1 =(User) a;
		User u2=(User) b;
		System.out.println(u1.getUname()+"--u1--"+u1.getFriendsCount()+"--------------FOFSort-----------------"+u2.getUname()+"---u2---"+u2.getFriendsCount());
		int result =u1.getUname().compareTo(u2.getUname());
		if(result==0){
			return -Integer.compare(u1.getFriendsCount(), u2.getFriendsCount());
		}
		return result;
	}
}
