/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.generator.DataGenerator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

final class DefaultFileHelper extends FileHelper {
  private static final String GZIP_EXTENSION = ".gz";

  private int fileCount = 0;

  DefaultFileHelper(Stage.Context context, S3TargetConfigBean s3TargetConfigBean, TransferManager transferManager) {
    super(context, s3TargetConfigBean, transferManager);
  }

  private String getUniqueDateWithIncrementalFileName(String keyPrefix) {
    fileCount++;
    StringBuilder fileName = new StringBuilder();
    fileName = fileName.append(keyPrefix).append(fileCount);
    if (s3TargetConfigBean.compress) {
      fileName = fileName.append(GZIP_EXTENSION);
    }
    return fileName.toString();
  }

  @Override
  public List<Upload> handle(Iterator<Record> recordIterator, String keyPrefix) throws IOException, StageException {
    //For uniqueness
    keyPrefix += System.currentTimeMillis() + "-";

    int writtenRecordCount = 0;
    List<Upload> uploads = new ArrayList<>();

    ByRefByteArrayOutputStream bOut = new ByRefByteArrayOutputStream();
    // wrap with gzip compression output stream if required
    OutputStream out = (s3TargetConfigBean.compress)? new GZIPOutputStream(bOut) : bOut;

    DataGenerator generator = s3TargetConfigBean.getGeneratorFactory().getGenerator(out);
    Record currentRecord;
    while (recordIterator.hasNext()) {
      currentRecord = recordIterator.next();
      try {
        generator.write(currentRecord);
        writtenRecordCount++;
      } catch (StageException e) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                currentRecord,
                e.getErrorCode(),
                e.getParams()
            )
        );
      } catch (IOException e) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                currentRecord,
                Errors.S3_32,
                currentRecord.getHeader().getSourceId(),
                e.toString(),
                e
            )
        );
      }
    }
    generator.close();

    // upload file on Amazon S3 only if at least one record was successfully written to the stream
    if (writtenRecordCount > 0) {
      String fileName = getUniqueDateWithIncrementalFileName(keyPrefix);

      //Create and issue file close event record, but the events are thrown after the batch completion.
      S3Events.S3_OBJECT_WRITTEN
          .create(context)
          .with(BUCKET, s3TargetConfigBean.s3Config.bucket)
          .with(OBJECT_KEY, fileName)
          .createAndSend();

      // Avoid making a copy of the internal buffer maintained by the ByteArrayOutputStream by using
      // ByRefByteArrayOutputStream
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bOut.getInternalBuffer(), 0, bOut.size());
      Upload upload = doUpload(fileName, byteArrayInputStream, getObjectMetadata());
      uploads.add(upload);
    }

    return uploads;
  }

  /**
   * Subclass of ByteArrayOutputStream which exposed the internal buffer to help avoid making a copy of the buffer.
   *
   * Note that the buffer size may be greater than the actual data. Therefore use {@link #size()} method to determine
   * the actual size of data.
   */
  private static class ByRefByteArrayOutputStream extends ByteArrayOutputStream {
    byte[] getInternalBuffer() {
      return buf;
    }
  }
}
