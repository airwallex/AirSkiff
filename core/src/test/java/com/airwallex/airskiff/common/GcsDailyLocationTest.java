package com.airwallex.airskiff.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GcsDailyLocationTest {

  @Test
  public void testLatestPath() {
    String bucketName = "test";
    String prefix = "path/to/data";
    Storage storage = Mockito.mock(Storage.class);
    Page<Blob> page = (Page<Blob>) Mockito.mock(Page.class);
    Blob blob1 = Mockito.mock(Blob.class);
    Blob blob2 = Mockito.mock(Blob.class);
    List<Blob> blobs = new ArrayList<>();
    Collections.addAll(blobs, blob1, blob2);

    Mockito.when(storage.list(bucketName, Storage.BlobListOption.prefix(prefix))).thenReturn(page);
    Mockito.when(page.iterateAll()).thenReturn(blobs);
    Mockito.when(blob1.getName()).thenReturn(prefix + "/2020-01-31");
    Mockito.when(blob2.getName()).thenReturn(prefix + "/2020-02-01");

    GcsDailyLocation location = new GcsDailyLocation(storage, bucketName, prefix);
    String path = location.latestPath();

    assertEquals(path, "gs://test/path/to/data/2020-02-01");

    Blob blob3 = Mockito.mock(Blob.class);
    Blob blob4 = Mockito.mock(Blob.class);
    Blob blob5 = Mockito.mock(Blob.class);
    Collections.addAll(blobs, blob3, blob4, blob5);
    Mockito.when(blob3.getName()).thenReturn(prefix + "/2020-01-31" + "/file1");
    Mockito.when(blob4.getName()).thenReturn(prefix + "/2020-01-31" + "/file2");
    Mockito.when(blob5.getName()).thenReturn(prefix + "/2020-02-01" + "/2020-03-01/file3");

    path = location.latestPath();
    assertEquals(path, "gs://test/path/to/data/2020-02-01");
  }
}
