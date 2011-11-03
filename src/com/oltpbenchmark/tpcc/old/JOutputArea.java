/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.tpcc.old;

/*
 * JOutputArea - Simple output area for jTPCC
 *
 * Copyright (C) 2003, Raul Barbosa 
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.awt.Font;

import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.oltpbenchmark.util.SimplePrinter;

public class JOutputArea extends JScrollPane implements SimplePrinter {
	public final static long DEFAULT_MAX_CHARS = 20000, NO_CHAR_LIMIT = 0;

	private JTextArea jTextArea;
	private long counter;
	private long maxChars;

	public JOutputArea(String text) {
		super();

		setVerticalScrollBarPolicy(VERTICAL_SCROLLBAR_ALWAYS);
		setHorizontalScrollBarPolicy(HORIZONTAL_SCROLLBAR_AS_NEEDED);
		jTextArea = new JTextArea(text);
		jTextArea.setEditable(false);
		jTextArea.setFont(new Font("Courier New", Font.BOLD, 12));
		maxChars = DEFAULT_MAX_CHARS;
		getViewport().add(jTextArea);
	}

	public JOutputArea() {
		this("");
	}

	public void print(String text) {
		if (getText().length() > maxChars)
			this.clear();

		jTextArea.append(text);
		jTextArea.setCaretPosition(jTextArea.getText().length());
	}

	public void println(String text) {
		print(text + "\n");
	}

	public void clear() {
		jTextArea.setText("");
		jTextArea.setCaretPosition(0);
	}

	public String getText() {
		return jTextArea.getText();
	}

	public void setMaxChars(long maxChars) {
		this.maxChars = maxChars;
	}
}
