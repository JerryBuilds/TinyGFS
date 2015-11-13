package com.master;

import com.master.Node;

import java.util.ArrayList;


public class Tree {
	public Node root;
	
	public Tree(Metadata rootData) {
		root = new Node();
		root.SetData(rootData);
	}
	
	
}
