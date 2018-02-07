package com.lc.mr.friend;

import org.apache.hadoop.io.Text;

public class Fof extends Text{

    public Fof(){
        super();
    }

    public Fof(String a,String b){
        super(getFof(a,b));
    }

    public static String getFof(String a,String b){
        int fof = a.compareTo(b);

        if(fof < 0 ){
            return a+"\t"+b;
        }else{
            return b+"\t"+a;
        }

    }
}
