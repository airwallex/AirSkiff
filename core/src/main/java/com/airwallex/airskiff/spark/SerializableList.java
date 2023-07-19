package com.airwallex.airskiff.spark;

import java.io.Serializable;
import java.util.ArrayList;

public class SerializableList<T> implements Serializable {

  public SerializableList() {
    list = new ArrayList<T>();
  }

  public void setList(ArrayList<T> list) {
    this.list = list;
  }

  private ArrayList<T> list;

  public SerializableList(ArrayList<T> list) {
    this.list = list;
  }

  public ArrayList<T> getList() {
    return list;
  }
}
