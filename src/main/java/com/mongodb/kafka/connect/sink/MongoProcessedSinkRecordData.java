/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.kafka.connect.sink;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkConverter;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategyHelper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

final class MongoProcessedSinkRecordData {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoProcessedSinkRecordData.class);
  private static final SinkConverter SINK_CONVERTER = new SinkConverter();

  private final MongoSinkTopicConfig config;
  private final MongoNamespace namespace;
  private final SinkRecord sinkRecord;
  private final SinkDocument sinkDocument;
  private final WriteModel<BsonDocument> writeModel;
  private boolean isSkipSync = false; // Skip sync Postgres to MongoDB
  private Exception exception;

  MongoProcessedSinkRecordData(final SinkRecord sinkRecord, final MongoSinkConfig sinkConfig) {
    this.sinkRecord = sinkRecord;
    this.config = sinkConfig.getMongoSinkTopicConfig(sinkRecord.topic());
    this.isSkipSync = isSkip();
    if (this.isSkipSync) {
      this.namespace = null;
      this.writeModel = null;
      this.sinkDocument = null;
    } else {
      this.sinkDocument = combineObject();
      if (this.sinkDocument == null) {
        this.namespace = null;
        this.writeModel = null;
      } else {
        this.namespace = createNamespace();
        this.writeModel = createWriteModel();
      }
    }
  }

  public boolean isSkipSync() {
    return isSkipSync;
  }

  public MongoSinkTopicConfig getConfig() {
    return config;
  }

  public MongoNamespace getNamespace() {
    return namespace;
  }

  public SinkRecord getSinkRecord() {
    return sinkRecord;
  }

  public WriteModel<BsonDocument> getWriteModel() {
    return writeModel;
  }

  public Exception getException() {
    return exception;
  }

  // Xử lý vấn đề đồng bộ lặp vô tận.
  // Cần check sync_actor của việc thay đổi dữ liệu trong postgres là từ mongodb thì không cần phải
  // đồng bộ ngược từ postgres sang mongodb
  private boolean isSkip() {
    Object value = this.sinkRecord.value();
    if (value instanceof HashMap) {
      Map<String, Object> valueMap = (HashMap<String, Object>) value;
      return "mongodb".equals(valueMap.getOrDefault("_sync_actor", null));
    }
    return false;
  }

  private SinkDocument combineObject() {
    switch (sinkRecord.topic()) {
      case "syain_busyo":
        return getSinkRecordSyainBusyo();
      case "sagyo_wokmodel":
        return getSinkRecordSagyoWokmodel();
      case "sagyobunrui_sagyo":
        return getSinkRecordSagyobunruiM();
      case "class_tree":
        return getSinkRecordClassTree();
      default:
        return SINK_CONVERTER.convert(sinkRecord);
    }
  }

  private SinkDocument getSinkRecordClassTree() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    if ((Long) valueMap.getOrDefault("depth", "") != 1) {
      return null;
    }

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("higher").toString()));

    BsonDocument bodyDoc = new BsonDocument();

    BsonDocument lower_classes = new BsonDocument();

    lower_classes.append("_id", new BsonString(valueMap.getOrDefault("_id", "").toString()));
    lower_classes.append("class_id", new BsonString(valueMap.getOrDefault("lower", "").toString()));
    lower_classes.append(
        "deleted", new BsonString(valueMap.getOrDefault("deleted", "").toString()));
    bodyDoc.append(
        "lower_classes.".concat(valueMap.getOrDefault("_id", "").toString()), lower_classes);

    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("higher").toString()));
    //    bodyDoc.append(
    //        "update_user", new BsonString(valueMap.getOrDefault("update_user", "").toString()));
    bodyDoc.append(
        "update_date", new BsonDateTime((Long) (valueMap.getOrDefault("update_date", 0))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordSagyobunruiM() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("sagyo_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();

    BsonDocument sagyobunrui_m = new BsonDocument();

    sagyobunrui_m.append(
        "_id", new BsonString(valueMap.getOrDefault("sagyobunrui_m_id", "").toString()));
    bodyDoc.append("sagyobunrui_m", sagyobunrui_m);

    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("sagyo_id").toString()));
    //    bodyDoc.append(
    //        "update_user", new BsonString(valueMap.getOrDefault("update_user", "").toString()));
    //    bodyDoc.append(
    //        "update_date", new BsonDateTime((Long) (valueMap.getOrDefault("update_date", 0))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordSagyoWokmodel() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("sagyo_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();

    BsonDocument sagyo_wokmodel = SINK_CONVERTER.convert(sinkRecord).getValueDoc().orElse(null);
    bodyDoc.append("sagyo_wokmodel", sagyo_wokmodel);

    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("sagyo_id").toString()));
    bodyDoc.append(
        "update_user", new BsonString(valueMap.getOrDefault("update_user", "").toString()));
    bodyDoc.append(
        "update_date", new BsonDateTime((Long) (valueMap.getOrDefault("update_date", 0))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordSyainBusyo() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("syain_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    BsonDocument syainBusyo = new BsonDocument();

    syainBusyo.append("_id", new BsonString(valueMap.getOrDefault("_id", "").toString()));
    syainBusyo.append("class_id", new BsonString(valueMap.getOrDefault("class_id", "").toString()));
    syainBusyo.append(
        "syusyozoku_flag",
        new BsonBoolean(
            Boolean.parseBoolean(valueMap.getOrDefault("syusyozoku_flag", false).toString())));
    syainBusyo.append(
        "create_user", new BsonString(valueMap.getOrDefault("create_user", "").toString()));
    syainBusyo.append(
        "create_date", new BsonDateTime((Long) (valueMap.getOrDefault("create_date", 0))));
    syainBusyo.append(
        "update_user", new BsonString(valueMap.getOrDefault("update_user", "").toString()));
    syainBusyo.append(
        "update_date", new BsonDateTime((Long) (valueMap.getOrDefault("update_date", 0))));

    // Todo: Tìm kiếm và xoá các syain_busyo ở document syain khác nếu thay đổi syain_id (cái này
    // ưu
    // tiên sau, vì nghiệp vụ chưa xảy ra)
    bodyDoc.append("syain_busyos.".concat(valueMap.getOrDefault("_id", "").toString()), syainBusyo);
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("syain_id").toString()));
    bodyDoc.append(
        "update_user", new BsonString(valueMap.getOrDefault("update_user", "").toString()));
    bodyDoc.append(
        "update_date", new BsonDateTime((Long) (valueMap.getOrDefault("update_date", 0))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private MongoNamespace createNamespace() {
    String databaseName = config.values().get("database").toString();
    switch (sinkRecord.topic()) {
      case "syain_busyo":
        return new MongoNamespace(databaseName.concat(".syain"));
      case "sagyo_wokmodel":
      case "sagyobunrui_sagyo":
        return new MongoNamespace(databaseName.concat(".sagyo"));
      case "class_tree":
        return new MongoNamespace(databaseName.concat(".class"));
      default:
        return tryProcess(
            () ->
                Optional.of(config.getNamespaceMapper().getNamespace(sinkRecord, sinkDocument)))
            .orElse(null);
    }
  }

  private WriteModel<BsonDocument> createWriteModel() {
    return config.getCdcHandler().isPresent() ? buildWriteModelCDC() : buildWriteModel();
  }

  private WriteModel<BsonDocument> buildWriteModel() {
    return tryProcess(
            () -> {
              config
                  .getPostProcessors()
                  .getPostProcessorList()
                  .forEach(pp -> pp.process(sinkDocument, sinkRecord));
              return Optional.ofNullable(
                  WriteModelStrategyHelper.createWriteModel(config, sinkDocument));
            })
        .orElse(null);
  }

  private WriteModel<BsonDocument> buildWriteModelCDC() {
    return tryProcess(
            () -> config.getCdcHandler().flatMap(cdcHandler -> cdcHandler.handle(sinkDocument)))
        .orElse(null);
  }

  private <T> Optional<T> tryProcess(final Supplier<Optional<T>> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      exception = e;
      if (config.logErrors()) {
        LOGGER.error("Unable to process record {}", sinkRecord, e);
      }
      if (!(config.tolerateErrors() || config.tolerateDataErrors())) {
        throw e;
      }
    }
    return Optional.empty();
  }
}
