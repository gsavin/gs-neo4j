/*
 * This file is part of d3 <http://d3-project.org>.
 * 
 * d3 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * d3 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with d3.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Copyright 2010 - 2011 Guilhelm Savin
 */
package org.graphstream.neo4j.tools;

public class CacheCreationException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9153429396907917991L;

	public CacheCreationException(String msg, Object... args) {
		super(String.format(msg, args));
	}

	public CacheCreationException(Throwable cause) {
		super(cause);
	}
}
