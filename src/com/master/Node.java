package com.master;

import com.master.Metadata;
import com.master.DirectoryMD;
import com.master.FileMD;

import java.util.ArrayList;

public class Node {
	private Metadata data;
	private Node parent;
	private ArrayList<Node> children;
	
	public Node() {
		children = new ArrayList<Node>();
		parent = null;
	}
	
	public Metadata GetData() {
		return data;
	}
	
	public Node GetParent() {
		return parent;
	}
	
	public void SetData(Metadata nodeData) {
		data = nodeData;
	}
	
	public void SetName(String newname) {
		data.name = newname;
	}
	
	// adds a new child
	// returns the child
	public Node AddChild(Metadata childData) {
		Node childNode = new Node();
		childNode.parent = this;
		childNode.data = childData;
		childNode.children = new ArrayList<Node>();
		this.children.add(childNode);
		return childNode;
	}
	
	// iterates through children and removes children
	// returns true is success, false if child doesn't exist
	public boolean RemoveChild(String childName) {
		for (int i=0; i < children.size(); i++) {
			if (children.get(i).data.name.equals(childName)) {
				children.remove(i);
				return true;
			}
		}
		return false;
	}
	
	// traverses all parents and appends all strings to
	// return the full path
	public String GetFullPath() {
		//if the node is the root node
		if (data.name.equals("")) {
			return "";
		}
		return parent.GetFullPath() + '/' + data.name;
	}
	
	// iterates through children and returns child
	// if child doesn't exist, returns null
	public Node GetChild(String childName) {
		for (int i=0; i < children.size(); i++) {
			if (children.get(i).data.name.equals(childName)) {
				return children.get(i);
			}
		}
		return null;
	}
	
	// returns all children of current node
	public ArrayList<Node> GetChildren() {
		return children;
	}
	
	// returns all descendants of current node
	public ArrayList<Node> GetAllDescendants() {
		ArrayList<Node> descendants = new ArrayList<Node>();
		ArrayList<Node> tempdescendants = new ArrayList<Node>();
		Node temp = this;
		
		for (int i=0; i < children.size(); i++) {
			temp = children.get(i);
			tempdescendants = temp.GetAllDescendants();
			descendants.add(temp);
			for (int j=0; j < tempdescendants.size(); j++) {
				descendants.add(tempdescendants.get(j));
			}
		}
		
		return descendants;
	}
}