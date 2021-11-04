package com.airwallex.airskiff.common;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GcsDailyLocation implements Serializable {
  private final String bucket;
  // we expect the prefix to be the immediate preceding part of a date such as
  // `ABC/DEF` in `ABC/DEF/2021-02-01/`
  private final String prefix;
  private final Pattern pattern;

  private transient Storage storage;
  private final transient SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  // for testing
  protected GcsDailyLocation(Storage storage, String bucket, String prefix) {
    this.storage = storage;
    this.bucket = bucket;
    this.prefix = prefix;
    this.pattern = Pattern.compile(stripSlashes(prefix) + "/(\\d{4}-\\d{2}-\\d{2})/$");
  }

  public GcsDailyLocation(String bucket, String prefix) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.pattern = Pattern.compile(stripSlashes(prefix) + "/(\\d{4}-\\d{2}-\\d{2})/$");
  }

  private String stripSlashes(String path) {
    if (path.startsWith("/")) {
      return stripSlashes(path.substring(1));
    } else if (path.endsWith("/")) {
      return stripSlashes(path.substring(0, path.length() - 1));
    }
    return path;
  }

  public String latestPath() {
    if (storage == null) {
      storage = StorageOptions.getDefaultInstance().getService();
    }
    Page<Blob> pages = storage.list(bucket, Storage.BlobListOption.prefix(prefix));
    List<Pair<Date, Blob>> dirs = new ArrayList<>();
    for (Blob blob : pages.iterateAll()) {
      String p = stripSlashes(blob.getName()) + "/";
      Matcher matcher = pattern.matcher(p);
      if (matcher.matches()) {
        String date = matcher.group(1);
        try {
          dirs.add(new Pair<>(format.parse(date), blob));
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
    }

    dirs.sort(Comparator.comparing(o -> o.l));
    return "gs://" + bucket + "/" + dirs.get(dirs.size() - 1).r.getName();
  }
}
