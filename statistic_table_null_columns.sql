DROP PROCEDURE IF EXISTS  `statistic_table_null_columns`;
DELIMITER $$
CREATE PROCEDURE statistic_table_null_columns(IN dbname VARCHAR(50), IN tablename VARCHAR(50))
BEGIN
	DECLARE i INT;
	DECLARE temp_str VARCHAR(50) DEFAULT '';
	DECLARE random_str VARCHAR(40) DEFAULT 'abcdefghijklmnopqrstuvwxyz1234567890';
    -- 创建临时表存储统计结果
	SET i = 1;
	WHILE i < 5 DO
		SET temp_str = CONCAT(temp_str, SUBSTRING(random_str, FLOOR(1 + RAND() * 36), 1));
		SET i = i + 1;
	END WHILE;
	SET @temp_table_name = CONCAT('statistic_table_null_col_', temp_str);
	SET @temp_table_creator = CONCAT('CREATE TABLE IF NOT EXISTS `', @temp_table_name, '`(`col_name` VARCHAR(50), `null_count` INT(11))');
	PREPARE stmt FROM @temp_table_creator;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	-- 查询表列数量
	SET @col_num_querier = CONCAT(
		'SELECT COUNT(`column_name`) INTO @col_num FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=\'', dbname,
		'\' AND TABLE_NAME=\'', tablename, '\'');
	PREPARE stmt FROM @col_num_querier;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	-- 统计
	SET i = 0;
	WHILE i < @col_num DO
		SET @col_querier = CONCAT(
			'SELECT `column_name` INTO @col_name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=\'', dbname,
			'\' AND TABLE_NAME=\'', tablename, '\' LIMIT 1 OFFSET ', i);
		PREPARE stmt FROM @col_querier;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		SET @statistc_sql = CONCAT(
			'INSERT INTO `', @temp_table_name, 
			'` SELECT \'', @col_name, '\' AS `col_name`, COUNT(1) AS `null_count` FROM `', tablename,
			'` WHERE `', @col_name, '` IS NULL');
		PREPARE stmt FROM @statistc_sql;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		SET i = i + 1;
	END WHILE;
    -- 输出
    SET @output_sql = CONCAT('SELECT * FROM ', @temp_table_name);
    PREPARE stmt FROM @output_sql;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	-- 清理
	SET @clear_sql = CONCAT('DROP TABLE ', @temp_table_name);
    PREPARE stmt FROM @clear_sql;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END $$
-- HOW TO USE
CALL statistic_table_null_columns('database_name', 'table_name');
