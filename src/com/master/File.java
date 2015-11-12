package com.master;

import com.client.*;

import java.util.*;

public class File extends Location {
//	public ArrayList<ArrayList<RID>> cs1info;
	public LinkedList<RID> cs1info;
	public File() {
		cs1info = new LinkedList<RID>();
	}
}
