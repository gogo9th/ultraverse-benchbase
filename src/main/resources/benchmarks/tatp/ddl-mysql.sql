SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS access_info CASCADE;
DROP TABLE IF EXISTS call_forwarding CASCADE;
DROP TABLE IF EXISTS special_facility CASCADE;
DROP TABLE IF EXISTS subscriber CASCADE;

DROP TABLE IF EXISTS __ULTRAVERSE_PROCEDURE_HINT;

CREATE TABLE __ULTRAVERSE_PROCEDURE_HINT(callinfo VARCHAR(512) DEFAULT NULL) ENGINE = BLACKHOLE;

CREATE TABLE subscriber (
    s_id         integer     NOT NULL PRIMARY KEY,
    sub_nbr      varchar(15) NOT NULL UNIQUE,
    bit_1        tinyint,
    bit_2        tinyint,
    bit_3        tinyint,
    bit_4        tinyint,
    bit_5        tinyint,
    bit_6        tinyint,
    bit_7        tinyint,
    bit_8        tinyint,
    bit_9        tinyint,
    bit_10       tinyint,
    hex_1        tinyint,
    hex_2        tinyint,
    hex_3        tinyint,
    hex_4        tinyint,
    hex_5        tinyint,
    hex_6        tinyint,
    hex_7        tinyint,
    hex_8        tinyint,
    hex_9        tinyint,
    hex_10       tinyint,
    byte2_1      smallint,
    byte2_2      smallint,
    byte2_3      smallint,
    byte2_4      smallint,
    byte2_5      smallint,
    byte2_6      smallint,
    byte2_7      smallint,
    byte2_8      smallint,
    byte2_9      smallint,
    byte2_10     smallint,
    msc_location integer,
    vlr_location integer
)  ;

CREATE TABLE access_info (
    s_id    integer NOT NULL,
    ai_type tinyint NOT NULL,
    data1   smallint,
    data2   smallint,
    data3   varchar(3),
    data4   varchar(5),
    PRIMARY KEY (s_id, ai_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
)  ;

CREATE TABLE special_facility (
    s_id        integer NOT NULL,
    sf_type     tinyint NOT NULL,
    is_active   tinyint NOT NULL,
    error_cntrl smallint,
    data_a      smallint,
    data_b      varchar(5),
    PRIMARY KEY (s_id, sf_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
)  ;

CREATE TABLE call_forwarding (
    s_id       integer NOT NULL,
    sf_type    tinyint NOT NULL,
    start_time tinyint NOT NULL,
    end_time   tinyint,
    numberx    varchar(15),
    PRIMARY KEY (s_id, sf_type, start_time),
    FOREIGN KEY (s_id, sf_type) REFERENCES special_facility (s_id, sf_type)
)  ;

CREATE INDEX idx_cf ON call_forwarding (s_id);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


DROP PROCEDURE IF EXISTS DeleteCallForwarding;
DELIMITER //
CREATE PROCEDURE DeleteCallForwarding(IN var_sub_nbr VARCHAR(15),
                                IN var_sf_type TINYINT,
                                IN var_start_time TINYINT)
            DeleteCallForwarding_Label:BEGIN

    DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
        UUID_SHORT(), 'DeleteCallForwarding',
        var_sub_nbr, var_sf_type, var_start_time
    );

                DECLARE var_s_id INT DEFAULT -1;

    INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

                SELECT s_id INTO var_s_id FROM subscriber WHERE sub_nbr = var_sub_nbr;
                DELETE FROM call_forwarding WHERE s_id = var_s_id AND sf_type = var_sf_type AND start_time = var_start_time;
END//
DELIMITER ;


DROP PROCEDURE IF EXISTS InsertCallForwarding;
DELIMITER //
CREATE PROCEDURE InsertCallForwarding(IN var_sub_nbr VARCHAR(15),
                                IN var_sf_type TINYINT,
                                IN var_start_time TINYINT,
                                IN var_end_time TINYINT,
                                IN var_numberx VARCHAR(15))
            InsertCallForwarding_Label:BEGIN

    DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
        UUID_SHORT(), 'InsertCallForwarding',
        var_sub_nbr, var_sf_type, var_start_time, var_end_time, var_numberx
    );

                DECLARE var_s_id INT DEFAULT -1;

    INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

                SELECT s_id INTO var_s_id  FROM subscriber WHERE sub_nbr = var_sub_nbr;
                SELECT sf_type FROM special_facility WHERE s_id = var_s_id;
                INSERT INTO call_forwarding (s_id, sf_type, start_time, end_time, numberx) VALUES (var_s_id, var_sf_type, var_start_time, var_end_time, var_numberx);
END//
DELIMITER ;



DROP PROCEDURE IF EXISTS UpdateLocation;
DELIMITER //
CREATE PROCEDURE UpdateLocation(IN loc INT,
                                IN var_sub_nbr VARCHAR(15))
            UpdateLocation_Label:BEGIN

    DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
        UUID_SHORT(), 'UpdateLocation',
        loc, var_sub_nbr
    );

                DECLARE var_s_id INT DEFAULT -1;

    INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

                SELECT s_id INTO var_s_id FROM subscriber WHERE sub_nbr = var_sub_nbr;
                UPDATE subscriber SET vlr_location = loc WHERE s_id = var_s_id;
END//
DELIMITER ;



DROP PROCEDURE IF EXISTS UpdateSubscriberData;
DELIMITER //
CREATE PROCEDURE UpdateSubscriberData(IN var_s_id INT,
                                IN var_bit_1 TINYINT,
                                IN var_data_a SMALLINT,
                                IN var_sf_type TINYINT)
            UpdateSubcriberData_Label:BEGIN

    DECLARE __ultraverse_callinfo VARCHAR(512) DEFAULT JSON_ARRAY(
        UUID_SHORT(), 'UpdateSubscriberData',
        var_s_id, var_bit_1, var_data_a, var_sf_type
    );

                DECLARE var_s_id INT DEFAULT -1;

    INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);

                UPDATE subscriber SET bit_1 = var_bit_1 WHERE s_id = var_s_id;
                UPDATE special_facility SET data_a = var_data_a WHERE s_id = var_s_id AND sf_type = var_sf_type;
END//
DELIMITER ;
