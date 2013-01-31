/*
 * This file is part of d3.
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
 * Copyright 2010 Guilhelm Savin
 */
package org.graphstream.neo4j.tools;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Cache<K, V> {

	private ReentrantLock lock;
	private HashMap<K, V> data;
	private LinkedList<K> availables;
	private final int capacity;

	public Cache(int capacity) {
		this.lock = new ReentrantLock();
		this.data = new HashMap<K, V>();
		this.availables = new LinkedList<K>();
		this.capacity = capacity;
	}

	public V get(K key) throws CacheCreationException {
		V value = null;

		CacheCreationException cce = null;

		try {
			lock();

			int index = availables.indexOf(key);

			if (index < 0) {
				index = create(key);

				if (index < 0)
					return null;
			}

			moveToTop(index);

			value = data.get(key);
		} catch (CacheCreationException e) {
			cce = e;
		} finally {
			unlock();
		}

		if (cce != null)
			throw cce;

		return value;
	}

	public boolean has(K key) {
		return availables.contains(key);
	}

	private void moveToTop(int index) {
		if (index < availables.size() - 1) {
			K key = availables.remove(index);
			availables.push(key);
		}
	}

	public int put(K key, V value) {
		int index;
		lock();

		while (availables.size() >= capacity)
			pop();

		availables.add(key);
		data.put(key, value);

		index = availables.size() - 1;

		unlock();
		return index;
	}

	public void revokeKey(K key) {
		if (has(key)) {
			lock();
			availables.remove(key);
			data.remove(key);
			unlock();
		}
	}

	protected abstract V createObject(K key) throws CacheCreationException;

	private int create(K key) throws CacheCreationException {
		V value = createObject(key);

		if (value == null)
			return -1;

		return put(key, value);
	}

	private void pop() {
		K key = availables.poll();

		if (key != null)
			data.remove(key);
	}

	private void lock() {
		lock.lock();
	}

	private void unlock() {
		lock.unlock();
	}
}
