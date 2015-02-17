/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.auctionmark.util;

import java.io.BufferedReader;
import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.util.FileUtil;


public class CategoryParser {
    private static final Logger LOG = Logger.getLogger(CategoryParser.class);
	
	Map<String, Category> _categoryMap;
	private int _nextCategoryID;
	String _fileName;

	public CategoryParser(File file) {
	
		_categoryMap = new TreeMap<String, Category>();
		_nextCategoryID = 0;
		
		
		try {
			BufferedReader br = FileUtil.getReader(file);
			String strLine;
			while ((strLine = br.readLine()) != null) {		
				extractCategory(strLine);
				//System.out.println(strLine);
			}
		} catch (Exception ex) {
		    throw new RuntimeException("Failed to load in category file", ex);
		}

	}

	public void extractCategory(String s){
		String[] tokens = s.split("\t");
		int itemCount = Integer.parseInt(tokens[5]);
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<=4; i++){
			if(!tokens[i].trim().isEmpty()){
				sb.append(tokens[i].trim())
				  .append("/");	
			} else {
				break;
			}
		}
		String categoryName = sb.toString();
		if(categoryName.length() > 0){
			categoryName = categoryName.substring(0, categoryName.length() - 1);
		}
		
		addNewCategory(categoryName, itemCount, true);
	}
	
	public Category addNewCategory(String fullCategoryName, int itemCount, boolean isLeaf){
		Category category = null;
		Category parentCategory = null;
		
		String categoryName = fullCategoryName;
		String parentCategoryName = "";
		Integer parentCategoryID = null;
		
		if(categoryName.indexOf('/') != -1){
			int separatorIndex = fullCategoryName.lastIndexOf('/');
			parentCategoryName = fullCategoryName.substring(0, separatorIndex);
			categoryName = fullCategoryName.substring(separatorIndex + 1, fullCategoryName.length());
		}
		/*
		System.out.println("parentCat name = " + parentCategoryName);
		System.out.println("cat name = " + categoryName);
		*/
		if(_categoryMap.containsKey(parentCategoryName)){
			parentCategory = _categoryMap.get(parentCategoryName);
		} else if(!parentCategoryName.isEmpty()){
			parentCategory = addNewCategory(parentCategoryName, 0, false);
		}
		
		if(parentCategory!=null){
			parentCategoryID = parentCategory.getCategoryID();	
		}
		
		category = new Category(_nextCategoryID++,
		        categoryName,
				parentCategoryID, 
				itemCount, 
				isLeaf);
		
		_categoryMap.put(fullCategoryName, category);
		
		return category;
	}
	
	public Map<String, Category> getCategoryMap(){
		return _categoryMap;
	}
	
	public static void main(String args[]) throws Exception {	
		CategoryParser ebp = new CategoryParser(new File("bin/edu/brown/benchmark/auctionmark/data/categories.txt"));
		
		for (String key : ebp.getCategoryMap().keySet()){
			LOG.info(key + " : " + ebp.getCategoryMap().get(key).getCategoryID() + " : " + ebp.getCategoryMap().get(key).getParentCategoryID() + " : " + ebp.getCategoryMap().get(key).getItemCount());
		}
		//addNewCategory("001/123456/789", 0, true);
	}
}
