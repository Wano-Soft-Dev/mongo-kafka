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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink.converter;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.MongoClientSettings;

/** Used for converting Maps e.g. Schemaless Json */
class MapRecordConverter implements RecordConverter {

  @SuppressWarnings({"unchecked"})
  @Override
  public BsonDocument convert(final Schema schema, final Object value) {
    if (value == null) {
      throw new DataException("Value was null for JSON conversion");
    }
    return new Document((Map<String, Object>) value)
        .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
  }

  @SuppressWarnings({"unchecked"})
  public BsonDocument customConvert(
      final Schema schema, final Object value, final String namespace) {
    if (value == null) {
      throw new DataException("Value was null for JSON conversion");
    }
    Map<String, Object> map = (Map<String, Object>) value;
    setDateTimeToBsonDateTime(map, namespace);
    setDateToBsonDateTime(map, namespace);
    setDateToStringDate(map, namespace);
    setDefaultValueBsonDateTime(map);

    return new Document(map)
        .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
  }

  /** Chuyển đổi từ định dạng DateTime (kafka: long: 1730710177547) => BsonDateTime */
  @SuppressWarnings("unchecked")
  private void setDateTimeToBsonDateTime(Map<String, Object> namespace, final String tableName) {
    // Tìm thông tin bảng trong danh sách tables
    Map<String, Object> tableInfo =
        FIELD_DATE_TIME_TO_BSON_DATETIME.stream()
            .filter(table -> table.get("table").equals(tableName))
            .findFirst()
            .orElse(null);

    if (tableInfo == null) {
      System.out.println("No matching table found for table name: " + tableName);
      return; // Không có bảng phù hợp, thoát sớm
    }

    // Lấy danh sách các trường cần chuyển đổi
    Set<String> fieldsToConvert = new HashSet<>((List<String>) tableInfo.get("field"));

    // Duyệt qua các key-value trong namespace và chuyển đổi
    for (Map.Entry<String, Object> entry : namespace.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      // Kiểm tra nếu key thuộc danh sách cần chuyển đổi và value là số
      if (fieldsToConvert.contains(key) && value instanceof Number) {
        long valueAsLong = ((Number) value).longValue();

        BsonDateTime date = new BsonDateTime(valueAsLong);
        namespace.put(key, date); // Cập nhật giá trị trong namespace
      }
    }
  }

  /** Chuyển đổi từ định dạng Date (kafka: integer: 19817) => BsonDateTime */
  @SuppressWarnings("unchecked")
  private void setDateToBsonDateTime(Map<String, Object> namespace, final String tableName) {
    // Tìm thông tin bảng trong danh sách tables
    Map<String, Object> tableInfo =
        FIELD_DATE_TO_BSON_DATETIME.stream()
            .filter(table -> table.get("table").equals(tableName))
            .findFirst()
            .orElse(null);

    if (tableInfo == null) {
      System.out.println("No matching table found for table name: " + tableName);
      return; // Không có bảng phù hợp, thoát sớm
    }

    // Lấy danh sách các trường cần chuyển đổi
    Set<String> fieldsToConvert = new HashSet<>((List<String>) tableInfo.get("field"));

    // Duyệt qua các key-value trong namespace và chuyển đổi
    for (Map.Entry<String, Object> entry : namespace.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      // Kiểm tra nếu key thuộc danh sách cần chuyển đổi và value là số
      if (fieldsToConvert.contains(key) && value instanceof Number) {
        long valueAsLong = ((Number) value).longValue();

        // Giả sử giá trị này là số ngày kể từ ngày 1970-01-01
        long newDate =
            24 * 60 * 60 * 1000L * valueAsLong; // Số mili giây trong một ngày * giá trị ngày
        BsonDateTime date = new BsonDateTime(newDate);
        namespace.put(key, date); // Cập nhật giá trị trong namespace
      }
    }
  }

  /** Chuyển đổi từ định dạng Date (kafka: integer: 19817) => StringDate: "2024-11-12T00:00:00Z" */
  @SuppressWarnings("unchecked")
  private void setDateToStringDate(Map<String, Object> namespace, final String tableName) {
    // Tìm thông tin bảng trong danh sách tables
    Map<String, Object> tableInfo =
        FIELD_DATE_TO_STRING_DATE.stream()
            .filter(table -> table.get("table").equals(tableName))
            .findFirst()
            .orElse(null);

    if (tableInfo == null) {
      System.out.println("No matching table found for table name: " + tableName);
      return; // Không có bảng phù hợp, thoát sớm
    }

    // Lấy danh sách các trường cần chuyển đổi
    Set<String> fieldsToConvert = new HashSet<>((List<String>) tableInfo.get("field"));

    // Duyệt qua các key-value trong namespace và chuyển đổi
    for (Map.Entry<String, Object> entry : namespace.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      // Kiểm tra nếu key thuộc danh sách cần chuyển đổi và value là số
      if (fieldsToConvert.contains(key) && value instanceof Number) {
        long valueAsLong = ((Number) value).longValue();

        // Giả sử giá trị này là số ngày kể từ ngày 1970-01-01
        long newDate =
            24 * 60 * 60 * 1000L * valueAsLong; // Số mili giây trong một ngày * giá trị ngày
        Date date = new Date(newDate);
        String formattedDate = ISO_FORMAT.format(date);

        namespace.put(key, formattedDate); // Cập nhật giá trị trong namespace
      }
    }
  }

  private void setDefaultValueBsonDateTime(Map<String, Object> map) {
    map.forEach(
        (key, value) -> {
          if (DATE_KEYS.contains(key) && value instanceof Number) {
            map.put(key, new BsonDateTime(((Number) value).longValue()));
          }
        });
  }

  private static Map<String, Object> createTableMap(String tableName, String... fields) {
    Map<String, Object> tableMap = new HashMap<>();
    tableMap.put("table", tableName);
    tableMap.put("field", Arrays.asList(fields));
    return tableMap;
  }

  private static final SimpleDateFormat ISO_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  private static final Set<String> DATE_KEYS =
      new HashSet<>(Arrays.asList("create_date", "update_date", "yukokikan_from", "yukokikan_to"));

  /** Chuyển đổi từ định dạng DateTime (kafka: long: 1730710177547) => BsonDateTime */
  private static final List<Map<String, Object>> FIELD_DATE_TIME_TO_BSON_DATETIME =
      Arrays.asList(
          createTableMap("seihin_recept_genpon", "tekiyou_kikan_from", "tekiyou_kikan_to"),
          createTableMap("husokuninji_send_genpon", "hiduke"),
          createTableMap("tenpo_seizo_jikantai_recept_genpon", "seizou_bi"),
          createTableMap("syain", "nyusya_date", "taisya_date", "ido_date"),
          //          createTableMap("demands", "hiduke", "hani_hiduke_from", "hani_hiduke_to"),
          createTableMap("task", "hiduke"),
          createTableMap("yosanninji_recept_genpon", "yosan_taisyo_day"),
          createTableMap("kykammokuhyo", "hiduke"),
          createTableMap(
              "bmail_recept_genpon",
              "jissi_syu_from",
              "jissi_syu_to",
              "koukai_kaishi_bi",
              "jisshi_kigen",
              "sagyou_bi",
              "yobi_bi_1",
              "yobi_bi_2",
              "yobi_bi_3",
              "yobi_bi_4",
              "yobi_bi_5",
              "yobi_bi_6",
              "hasshin_nichiji",
              "koushin_nichiji"),
          createTableMap(
              "be_bmail",
              "jissi_syu_from",
              "jissi_syu_to",
              "koukai_kaishi_bi",
              "jisshi_kigen",
              "sagyou_bi",
              "yobi_bi_1",
              "yobi_bi_2",
              "yobi_bi_3",
              "yobi_bi_4",
              "yobi_bi_5",
              "yobi_bi_6",
              "hasshin_nichiji",
              "koushin_nichiji"));

  /** Chuyển đổi từ định dạng Date (kafka: integer: 19817) => BsonDateTime */
  private static final List<Map<String, Object>> FIELD_DATE_TO_BSON_DATETIME =
      Arrays.asList(
          createTableMap("table_name_1", "field_1", "field_2"),
          createTableMap("demands", "hani_hiduke_from", "hani_hiduke_to"));

  /** Chuyển đổi từ định dạng Date (kafka: integer: 19817) => StringDate: "2024-11-12T00:00:00Z" */
  private static final List<Map<String, Object>> FIELD_DATE_TO_STRING_DATE =
      Arrays.asList(
          createTableMap("table_name_3", "field_1", "field_2"),
          createTableMap("demands", "hiduke"));
}
