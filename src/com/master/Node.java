package com.master;

import java.util.ArrayList;

public class Node<T> {
	private T data;
	private Node<T> parent;
	private ArrayList<Node<T>> children;
	
	public Node() {
		children = new ArrayList<Node<T>>();
		parent = null;
	}
	
	public T GetData() {
		return data;
	}
	
	public void SetData(T nodeData) {
		data = nodeData;
	}
	
	// adds a new child
	// returns the child
	public Node<T> AddChild(T childData) {
		Node<T> childNode = new Node<T>();
		childNode.parent = this;
		childNode.data = childData;
		childNode.children = new ArrayList<Node<T>>();
		this.children.add(childNode);
		return childNode;
	}
	
	// iterates through children and removes children
	// returns true is success, false if child doesn't exist
	public boolean RemoveChild(T childData) {
		for (int i=0; i < children.size(); i++) {
			if (children.get(i).equals(childData)) {
				children.remove(i);
				return true;
			}
		}
		return false;
	}
	
	// iterates through children and returns child
	// if child doesn't exist, returns null
	public Node<T> Get(T childData) {
		for (int i=0; i < children.size(); i++) {
			if (children.get(i).equals(childData)) {
				return children.get(i);
			}
		}
		return null;
	}
	
	public ArrayList<Node<T>> GetChildren() {
		return children;
	}
}