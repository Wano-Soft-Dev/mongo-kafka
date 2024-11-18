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

 import java.lang.annotation.Documented;
 import java.util.ArrayList;
 import java.util.HashMap;
 import java.util.Map;
 import java.util.Optional;
 import java.util.function.Supplier;
 
 import org.apache.kafka.connect.sink.SinkRecord;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import org.bson.BsonArray;
 import org.bson.BsonBoolean;
 import org.bson.BsonDocument;
 import org.bson.BsonString;
 import org.bson.BsonValue;
 
 import com.mongodb.MongoNamespace;
 import com.mongodb.client.model.WriteModel;
 
 import com.mongodb.kafka.connect.sink.converter.SinkConverter;
 import com.mongodb.kafka.connect.sink.converter.SinkDocument;
 import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategyHelper;
 
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
     if (this.isSkipSync == true) {
       this.namespace = null;
       this.writeModel = null;
       this.sinkDocument = null;
     } else {
       this.namespace = createNamespace();
       this.writeModel = createWriteModel();
       this.sinkDocument = combineObject();
     }
   }
   
   public boolean isSkipSync(){
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
   // Cần check sync_actor của việc thay đổi dữ liệu trong postgres là từ mongodb thì không cần phải đồng bộ ngược từ postgres sang mongodb
   private boolean isSkip(){
     Object value = this.sinkRecord.value();
     if (value instanceof HashMap) {
       Map<String, Object> valueMap = (HashMap<String, Object>) value;
       if ("mongodb".equals(valueMap.getOrDefault("sync_actor", null))) {
         return true;
       }
     }
     return false;
   }
 
   private SinkDocument combineObject(){
     if (sinkRecord.topic().equals("syain_busyo")){
       Map<String, Object> valueMap = (HashMap<String, Object>) sinkRecord.value();
 
       BsonDocument keyDoc = new BsonDocument();
       keyDoc.append("id", new BsonString(valueMap.get("syain_id").toString()));
       
       BsonDocument bodyDoc = new BsonDocument();
       BsonDocument syainBusyo = new BsonDocument();
       
       syainBusyo.append("_id", new BsonString(valueMap.getOrDefault("id", "").toString()));
       syainBusyo.append("class_id", new BsonString(valueMap.getOrDefault("class_id", "").toString()));
       syainBusyo.append("syusyozoku_flag", new BsonBoolean(Boolean.parseBoolean(valueMap.getOrDefault("syusyozoku_flag", false).toString())));
       syainBusyo.append("create_user", new BsonString(valueMap.getOrDefault("create_user", "").toString()));
       
       // Todo: cần query xuống mongodb để lấy ra List Syain_busyo hiện tại. Tìm kiếm và xoá các syain_busyo ở document syain khác nếu thay đổi sayin
       BsonArray syainBusyoArray = new BsonArray();
       syainBusyoArray.add(syainBusyo);
       bodyDoc.append("syain_busyo", syainBusyoArray);
       
       // Todo: Cập nhật các trưòng update_user, update_date cho document syain.
 
       SinkDocument sayainSinkDoc = new SinkDocument(keyDoc, bodyDoc);
       return sayainSinkDoc;
     }
     
     return SINK_CONVERTER.convert(sinkRecord);
   }
 
   private MongoNamespace createNamespace() {
     return tryProcess(
             () -> Optional.of(config.getNamespaceMapper().getNamespace(sinkRecord, sinkDocument)))
         .orElse(null);
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
 