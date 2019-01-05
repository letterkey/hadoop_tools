package test;

import java.util.Stack;

public class StackTest {
	public static void main(String[] args) {  
        Stack<String> stack = new Stack<String>();  
        System.out.println("now the stack is " + isEmpty(stack));  
        stack.push("1");  
        stack.push("1");  
        stack.push("2");  
        stack.push("3");  
        stack.push("4"); 
        stack.push("5");  
        System.out.println("now the stack is " + isEmpty(stack)); 

        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.pop());  
        System.out.println(stack.search("2"));  
    }  
    public static String isEmpty(Stack<String> stack) {  
        return stack.empty() ? "empty" : "not empty";  
    }  
}
