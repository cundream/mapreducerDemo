package com.lc.mr.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable<MyKey>{
	private int year;
	private int month;
	private double hot;
	//自定义key 输入输出的时用  要序列化  和反序列化   实现Writable就行
	//做键的话要 重写eques方法  WritableComparable
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public double getHot() {
		return hot;
	}
	public void setHot(double hot) {
		this.hot = hot;
	}
	
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.year=arg0.readInt();
		this.month=arg0.readInt();
		this.hot=arg0.readDouble();
	}
	
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(year);
		arg0.writeInt(month);
		arg0.writeDouble(hot);
		
	}
	//判断对象是否为同一个对象，什么时候做判断，单该底下作为输出的key
	public int compareTo(MyKey o) {
		// TODO Auto-generated method stub
		int year = Integer.compare(this.year, o.getYear());
		if(year == 0){
			int month = Integer.compare(this.month, o.getMonth());
			if(month == 0){
				return Double.compare(this.hot, o.getHot());
			}else{
				return month;
			}
		}else{
			return year;
		}
	}

	

}
