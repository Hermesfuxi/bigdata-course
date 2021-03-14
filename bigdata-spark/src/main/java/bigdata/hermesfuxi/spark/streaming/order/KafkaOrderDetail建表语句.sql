 -- phoenix:  建表语句：
 CREATE TABLE "doit.spark_kafka_order"
 (RK VARCHAR PRIMARY KEY,
 "orders"."cid" UNSIGNED_INT,
 "orders"."money" UNSIGNED_DOUBLE,
 "orders"."longitude" UNSIGNED_DOUBLE,
 "orders"."latitude" UNSIGNED_DOUBLE,
 "offsets"."groupid" VARCHAR,
 "offsets"."topic" VARCHAR,
 "offsets"."partition" UNSIGNED_INT,
 "offsets"."offset" UNSIGNED_LONG)
 COLUMN_ENCODED_BYTES='NONE';

 -- phoenix 查询偏移量语句：
select "topic", "partition", max("offset") as "offset" from "spark_kafka_order" where "groupid" = ? group by "topic", "partition"

-- mysql:  建表语句：
DROP TABLE IF EXISTS `spark_kafka_order`;
CREATE TABLE `spark_kafka_order`  (
  `oid` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci  NOT NULL,
  `cid` int(0),
  `money` double(10, 2),
  `longitude` double(10, 2),
  `latitude` double(10, 2),
  PRIMARY KEY (`oid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;
SET FOREIGN_KEY_CHECKS = 1;

DROP TABLE IF EXISTS `kafka_offset`;
CREATE TABLE `kafka_offset`  (
  `group_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `topic` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `partition` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `offset` bigint(0) NULL DEFAULT NULL,
  PRIMARY KEY (`group_id`, `topic`, `partition`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;
SET FOREIGN_KEY_CHECKS = 1;

