package com.airwallex.airskiff.testhelpers;

import com.airwallex.airskiff.common.functions.NamedSerializableLambda;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class KList<K, T> implements List<T> {
  public final NamedSerializableLambda<T, K> toKey;
  private final List<T> internal;

  public KList(List<T> internal, NamedSerializableLambda<T, K> toKey) {
    this.internal = internal;
    this.toKey = toKey;
  }

  @Override
  public int size() {
    return internal.size();
  }

  @Override
  public boolean isEmpty() {
    return internal.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return internal.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return internal.iterator();
  }

  @Override
  public Object[] toArray() {
    return internal.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return internal.toArray(a);
  }

  @Override
  public boolean add(T t) {
    return internal.add(t);
  }

  @Override
  public boolean remove(Object o) {
    return internal.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return internal.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return internal.addAll(c);
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    return internal.addAll(index, c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return internal.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return internal.retainAll(c);
  }

  @Override
  public void clear() {
    internal.clear();
  }

  @Override
  public T get(int index) {
    return internal.get(index);
  }

  @Override
  public T set(int index, T element) {
    return internal.set(index, element);
  }

  @Override
  public void add(int index, T element) {
    internal.add(index, element);
  }

  @Override
  public T remove(int index) {
    return internal.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    return internal.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return internal.lastIndexOf(o);
  }

  @Override
  public ListIterator<T> listIterator() {
    return internal.listIterator();
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return internal.listIterator(index);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return internal.subList(fromIndex, toIndex);
  }
}
