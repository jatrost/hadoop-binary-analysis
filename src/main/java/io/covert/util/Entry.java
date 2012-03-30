package io.covert.util;

public class Entry<K, V> implements java.util.Map.Entry<K, V> {

	K key;
	V val;
	
	public Entry(K key, V val)
	{
		this.key = key;
		this.val = val;
	}
	
	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return val;
	}

	@Override
	public V setValue(V value) {
		V oldVal = val;
		val = value;
		return oldVal;
	}

}
