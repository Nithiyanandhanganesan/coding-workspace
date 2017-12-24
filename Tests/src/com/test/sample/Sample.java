package com.test.sample;

import java.util.ArrayList;
import java.util.List;

public class Sample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			List<String> list1 = new ArrayList<String>();
			list1.add("one");
			list1.add("two");
			list1.add("three");
			List<String> list2 = new ArrayList<String>();
			list2.addAll(list1);
			
			for(String s1:list2){
				list2.add("String");
			}
			
			System.out.println(list2);
			
		}catch(Exception e){
			e.printStackTrace();
		}

	}

}
