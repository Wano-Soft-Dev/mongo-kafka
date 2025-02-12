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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.*;
import org.bson.conversions.Bson;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkConverter;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategyHelper;

final class MongoProcessedSinkRecordData {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoProcessedSinkRecordData.class);
  private static final SinkConverter SINK_CONVERTER = new SinkConverter();

  private final MongoClient mongoClient;
  private final MongoSinkTopicConfig config;
  private final MongoNamespace namespace;
  private final SinkRecord sinkRecord;
  private final SinkDocument sinkDocument;
  private final WriteModel<BsonDocument> writeModel;
  private boolean isSkipSync = false; // Skip sync Postgres to MongoDB
  private Exception exception;

  private static long currentTime = System.currentTimeMillis();

  MongoProcessedSinkRecordData(
      final SinkRecord sinkRecord,
      final MongoSinkConfig sinkConfig,
      final MongoClient mongoClient) {
    this.sinkRecord = sinkRecord;
    this.config = sinkConfig.getMongoSinkTopicConfig(sinkRecord.topic());
    this.isSkipSync = isSkip();
    if (this.isSkipSync) {
      this.namespace = null;
      this.writeModel = null;
      this.sinkDocument = null;
      this.mongoClient = null;
    } else {
      this.mongoClient = mongoClient;
      this.sinkDocument = combineObject();
      this.namespace = createNamespace();
      this.writeModel = createWriteModel();
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
      return SYNC_ACTOR_MONGODB.equals(valueMap.getOrDefault(SYNC_ACTOR_FIELD, null));
    }
    return false;
  }

  private MongoNamespace createNamespace() {
    if (this.isSkipSync) return null;

    String databaseName = config.values().get("database").toString();
    switch (sinkRecord.topic()) {
      case "syain_busyo":
        return new MongoNamespace(databaseName.concat(".syain"));
      case "sagyo_wokmodel":
      case "sagyobunrui_sagyo":
        return new MongoNamespace(databaseName.concat(".sagyo"));
      case "class_tree":
        return new MongoNamespace(databaseName.concat(".class"));
      case "classgroup_rel":
        return new MongoNamespace(databaseName.concat(".class_group"));
      case "tenpogroup_rel":
      case "tenpogroup_class_rel":
        return new MongoNamespace(databaseName.concat(".tenpo_group"));
      case "task":
        return new MongoNamespace(databaseName.concat(".shift"));
      case "workschedule":
        return new MongoNamespace(databaseName.concat(".demands"));
      default:
        return tryProcess(
                () ->
                    Optional.of(config.getNamespaceMapper().getNamespace(sinkRecord, sinkDocument)))
            .orElse(null);
    }
  }

  private SinkDocument combineObject() {
    if (this.isSkipSync) return null;

    SinkDocument result;
    switch (sinkRecord.topic()) {
      case "syain_busyo":
        result = getSinkRecordSyainBusyo();
        break;
      case "sagyo_wokmodel":
        result = getSinkRecordSagyoWokmodel();
        break;
      case "sagyobunrui_sagyo":
        result = getSinkRecordSagyobunruiM();
        break;
      case "class_tree":
        result = getSinkRecordClassTree();
        break;
      case "classgroup_rel":
        result = getSinkRecordClassgroupRel();
        break;
      case "tenpogroup_rel":
        result = getSinkRecordTenpogroupRel();
        break;
      case "tenpogroup_class_rel":
        result = getSinkRecordTenpogroupClassRel();
        break;
      case "task":
        result = getSinkRecordTask();
        break;
      case "workschedule":
        result = getSinkRecordWorkschedule();
        break;
      case "demands":
        result = getSinkRecordDemands();
        break;
      default:
        result = SINK_CONVERTER.convert(sinkRecord);
        break;
    }
    if (this.isSkipSync) return null;

    // set default _sync_actor
    BsonDocument bodyDoc = result.getValueDoc().orElse(null);
    if (bodyDoc == null || bodyDoc.isEmpty()) return result;

    bodyDoc.append(SYNC_ACTOR_FIELD, new BsonString(SYNC_ACTOR_POSTGRES));
    return new SinkDocument(result.getKeyDoc().orElse(null), bodyDoc);
  }

  private SinkDocument getSinkRecordTenpogroupClassRel() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("tenpogroupId").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("tenpogroupId").toString()));

    BsonDocument tenporoupClassRel = SINK_CONVERTER.convert(sinkRecord).getValueDoc().orElse(null);
    bodyDoc.append(
        "tenporoup_class_rel.".concat(valueMap.getOrDefault("_id", "").toString()),
        tenporoupClassRel);

    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date",
        tenporoupClassRel.getOrDefault("update_date", new BsonDateTime(currentTime)));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordTenpogroupRel() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    if ((Long) valueMap.getOrDefault("depth", "") != 1) {
      return null;
    }

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("higher").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("higher").toString()));

    BsonDocument lowerTenpoGroups = new BsonDocument();

    lowerTenpoGroups.append("_id", new BsonString(valueMap.getOrDefault("_id", "").toString()));
    lowerTenpoGroups.append(
        "tenpo_group_id", new BsonString(valueMap.getOrDefault("lower", "").toString()));
    lowerTenpoGroups.append(
        "deleted",
        new BsonBoolean(Boolean.parseBoolean(valueMap.getOrDefault("deleted", false).toString())));
    bodyDoc.append(
        "lower_tenpo_groups.".concat(valueMap.getOrDefault("_id", "").toString()),
        lowerTenpoGroups);

    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("update_date", currentTime))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordClassgroupRel() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    if ((Long) valueMap.getOrDefault("depth", "") != 1) {
      isSkipSync = true;
      return null;
    }

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("higher").toString()));

    BsonDocument bodyDoc = new BsonDocument();

    BsonDocument lowerClassGroups = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("higher").toString()));

    lowerClassGroups.append("_id", new BsonString(valueMap.getOrDefault("_id", "").toString()));
    lowerClassGroups.append(
        "class_group_id", new BsonString(valueMap.getOrDefault("lower", "").toString()));
    lowerClassGroups.append(
        "deleted",
        new BsonBoolean(Boolean.parseBoolean(valueMap.getOrDefault("deleted", false).toString())));
    bodyDoc.append(
        "lower_classgroups.".concat(valueMap.getOrDefault("_id", "").toString()), lowerClassGroups);

    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("update_date", currentTime))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordClassTree() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    if ((Long) valueMap.getOrDefault("depth", "") != 1) {
      isSkipSync = true;
      return null;
    }

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("higher").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("higher").toString()));

    BsonDocument lowerClasses = new BsonDocument();

    lowerClasses.append("_id", new BsonString(valueMap.getOrDefault("_id", "").toString()));
    lowerClasses.append("class_id", new BsonString(valueMap.getOrDefault("lower", "").toString()));
    lowerClasses.append(
        "deleted",
        new BsonBoolean(Boolean.parseBoolean(valueMap.getOrDefault("deleted", false).toString())));
    bodyDoc.append(
        "lower_classes.".concat(valueMap.getOrDefault("_id", "").toString()), lowerClasses);

    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("update_date", currentTime))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordSagyobunruiM() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("sagyo_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("sagyo_id").toString()));

    BsonDocument sagyobunruiM = new BsonDocument();

    sagyobunruiM.append(
        "_id", new BsonString(valueMap.getOrDefault("sagyobunrui_m_id", "").toString()));
    sagyobunruiM.append(
        "sagyobunrui_m_id",
        new BsonString(valueMap.getOrDefault("sagyobunrui_m_id", "").toString()));
    sagyobunruiM.append(
        "deleted",
        new BsonBoolean(Boolean.parseBoolean(valueMap.getOrDefault("deleted", false).toString())));

    bodyDoc.append("sagyobunrui_m", sagyobunruiM);

    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("update_date", currentTime))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordSagyoWokmodel() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("sagyo_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("sagyo_id").toString()));

    BsonDocument sagyoWokmodel = SINK_CONVERTER.convert(sinkRecord).getValueDoc().orElse(null);
    bodyDoc.append("sagyo_wokmodel", sagyoWokmodel);

    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date", sagyoWokmodel.getOrDefault("update_date", new BsonDateTime(currentTime)));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordSyainBusyo() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("syain_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("syain_id").toString()));

    BsonDocument syainBusyo = new BsonDocument();

    syainBusyo.append("_id", new BsonString(valueMap.getOrDefault("_id", "").toString()));
    syainBusyo.append("class_id", new BsonString(valueMap.getOrDefault("class_id", "").toString()));
    syainBusyo.append(
        "syusyozoku_flag",
        new BsonBoolean(
            Boolean.parseBoolean(valueMap.getOrDefault("syusyozoku_flag", false).toString())));
    syainBusyo.append(
        "create_user", new BsonString(Objects.toString(valueMap.get("create_user"), "connector")));
    syainBusyo.append(
        "yuko_kikan_kaishi",
        new BsonDateTime((Long) (valueMap.getOrDefault("yuko_kikan_kaishi", currentTime))));
    syainBusyo.append(
        "yuko_kikan_shuryo",
        new BsonDateTime((Long) (valueMap.getOrDefault("yuko_kikan_shuryo", currentTime))));
    syainBusyo.append(
        "create_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("create_date", currentTime))));
    syainBusyo.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    syainBusyo.append(
        "update_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("update_date", currentTime))));

    // Todo: Tìm kiếm và xoá các syain_busyo ở document syain khác nếu thay đổi syain_id (cái này
    // ưu tiên sau, vì nghiệp vụ chưa xảy ra)
    bodyDoc.append("syain_busyos.".concat(valueMap.getOrDefault("_id", "").toString()), syainBusyo);
    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date",
        new BsonDateTime((Long) (valueMap.getOrDefault("update_date", currentTime))));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordTask() {
    Map<String, Object> taskData = (HashMap<String, Object>) sinkRecord.value();
    String databaseName = config.values().get("database").toString();
    MongoCollection<Document> collection =
        this.mongoClient.getDatabase(databaseName).getCollection("shift");

    Bson query =
        Filters.and(
            Filters.eq("syain_id", taskData.get("syain_id").toString()),
            // Filters.eq("hiduke", taskData.get("hiduke").toString()), // Todo: need uncomment
            Filters.lte("kinmu_from", taskData.get("kinmu_from").toString()),
            Filters.gte("kinmu_to", taskData.get("kinmu_to").toString()));
    // Get matching documents
    Document shiftDocument = collection.find(query).first();
    if (shiftDocument == null) {
      System.out.println("No document shift matches the query." + query.toString());
      this.isSkipSync = true;
      return null;
    }

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(shiftDocument.get("_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(shiftDocument.get("_id").toString()));

    BsonDocument taskDoc = SINK_CONVERTER.convert(sinkRecord).getValueDoc().orElse(null);

    bodyDoc.append("tasks." + taskData.getOrDefault("task_id", "").toString(), taskDoc);

    bodyDoc.append(
        "create_user", new BsonString(Objects.toString(taskData.get("create_user"), "connector")));
    bodyDoc.append(
        "create_date", taskDoc.getOrDefault("create_date", new BsonDateTime(currentTime)));
    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(taskData.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date", taskDoc.getOrDefault("update_date", new BsonDateTime(currentTime)));

    return new SinkDocument(keyDoc, bodyDoc);

    // Todo: add task to demands.workschedules.task_id_list
  }

  private SinkDocument getSinkRecordWorkschedule() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("demand_id").toString()));

    BsonDocument bodyDoc = new BsonDocument();
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("demand_id").toString()));

    BsonDocument workschedule = SINK_CONVERTER.convert(sinkRecord).getValueDoc().orElse(null);

    bodyDoc.append(
        "workschedules.".concat(valueMap.getOrDefault("workschedule_id", "").toString()),
        workschedule);
    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    bodyDoc.append(
        "update_date", workschedule.getOrDefault("update_date", new BsonDateTime(currentTime)));

    return new SinkDocument(keyDoc, bodyDoc);
  }

  private SinkDocument getSinkRecordDemands() {
    Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();

    BsonDocument keyDoc = new BsonDocument();
    keyDoc.append("id", new BsonString(valueMap.get("demand_id").toString()));

    BsonDocument bodyDoc = SINK_CONVERTER.convert(sinkRecord).getValueDoc().orElse(null);
    bodyDoc.append(ID_FIELD, new BsonString(valueMap.get("demand_id").toString()));

    bodyDoc.append(
        "create_user", new BsonString(Objects.toString(valueMap.get("create_user"), "connector")));
    bodyDoc.append(
        "update_user", new BsonString(Objects.toString(valueMap.get("update_user"), "connector")));
    return new SinkDocument(keyDoc, bodyDoc);
  }

  private WriteModel<BsonDocument> createWriteModel() {
    if (this.isSkipSync) return null;
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
