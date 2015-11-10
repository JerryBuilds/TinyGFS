package com.master;

import com.master.Node;

import java.util.ArrayList;


public class Tree<T> {
	public Node<T> root;
	
	public Tree(T rootData) {
		root = new Node<T>();
		root.SetData(rootData);
	}
	
	
}
