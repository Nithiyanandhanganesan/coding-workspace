package com.test.sample;

import java.util.List;

class Parent{
	void method(List<? extends Number> input){
		
		List<String> l1 = null;
		List<Integer> l2 = null;
		
		l1=l2;
		for(Number n:input){
			
		}
	}
}
public class Test extends Parent{
	void method(List<Integer> input){
		
	}
		
}
