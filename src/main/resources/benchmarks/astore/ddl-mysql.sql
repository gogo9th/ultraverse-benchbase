SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS ProfileIdEdit CASCADE;
DROP TABLE IF EXISTS ProfileIdChangePassword CASCADE;
DROP TABLE IF EXISTS ProfileAddressAdd CASCADE;
DROP TABLE IF EXISTS ProfileAddressDelete CASCADE;
DROP TABLE IF EXISTS ProfileAddressEdit CASCADE;
DROP TABLE IF EXISTS RoutesSubscribe CASCADE;
DROP TABLE IF EXISTS CheckoutDeliveryNew CASCADE;
DROP TABLE IF EXISTS CheckoutOrder CASCADE;
DROP TABLE IF EXISTS ContactUs CASCADE;

CREATE TABLE `Categories` (
  `CategoryID`   INTEGER     NOT NULL AUTO_INCREMENT,
  `CategoryName` VARCHAR(58) NOT NULL,
  `Description`  MEDIUMTEXT,
  `CategorySlug` VARCHAR(68) NOT NULL,
  `Image`        VARCHAR(58) NOT NULL,
  CONSTRAINT `PK_Categories` PRIMARY KEY (`CategoryID`)
);

CREATE INDEX `CategoryName` ON `Categories` (`CategoryName`);

CREATE TABLE `Users` (
  `UserID`        INTEGER      NOT NULL AUTO_INCREMENT,
  `FullName`      VARCHAR(50)  NOT NULL,
  `StreetAddress` VARCHAR(255) NOT NULL,
  `PostCode`      VARCHAR(5)   NOT NULL,
  `City`          VARCHAR(28)  NOT NULL,
  `Country`       VARCHAR(28)  NOT NULL,
  `Phone`         VARCHAR(12)  NOT NULL,
  `Email`         VARCHAR(50)  NOT NULL,
  `Username`      VARCHAR(28),
  `Password`      VARCHAR(158),
  `Admin`         BOOLEAN      NOT NULL DEFAULT 0,
  CONSTRAINT `PK_Users` PRIMARY KEY (`UserID`)
);

CREATE INDEX `Username` ON `Users` (`Username`);

CREATE TABLE `Addresses` (
  `AddressID`     INTEGER      NOT NULL AUTO_INCREMENT,
  `UserID`        INTEGER,
  `FullName`      VARCHAR(50)  NOT NULL,
  `StreetAddress` VARCHAR(255) NOT NULL,
  `PostCode`      VARCHAR(5)   NOT NULL,
  `City`          VARCHAR(28)  NOT NULL,
  `Country`       VARCHAR(28)  NOT NULL,
  `Phone`         VARCHAR(12)  NOT NULL,
  CONSTRAINT `PK_Addresses` PRIMARY KEY (`AddressID`),
  CONSTRAINT `FK_Users_UserID` FOREIGN KEY (`UserID`) REFERENCES `Users` (`UserID`) ON DELETE CASCADE
);

/*
** Add table "Products"
*/

CREATE TABLE `Products` (
  `ProductID`       INTEGER      NOT NULL AUTO_INCREMENT,
  `ProductName`     VARCHAR(40)  NOT NULL,
  `CategoryID`      INTEGER,
  `ProductPrice`    DECIMAL(10, 2)        DEFAULT 0,
  `UnitsInStock`    SMALLINT(5)           DEFAULT 0,
  `Description`     VARCHAR(255) NOT NULL,
  `ManufactureYear` SMALLINT(5)  NOT NULL,
  `Image`           VARCHAR(50)  NOT NULL,
  `ProductSlug`     VARCHAR(50)  NOT NULL,
  `Feature`         BOOLEAN      NOT NULL DEFAULT 0,
  CONSTRAINT `PK_Products` PRIMARY KEY (`ProductID`),
  CONSTRAINT `FK_Products_Categories` FOREIGN KEY (`CategoryID`) REFERENCES `Categories` (`CategoryID`) ON DELETE CASCADE
);

CREATE INDEX `ProductName` ON `Products` (`ProductName`);

CREATE TABLE `Orders` (
  `OrderID`   INTEGER NOT NULL AUTO_INCREMENT,
  `UserID`    INTEGER NOT NULL,
  `AddressID` INTEGER NOT NULL,
  `SubTotal`  DECIMAL(10,2),
  `Discount`  DECIMAL(10,2),
  `ShippingFee`  DECIMAL(10,2),
  `Total`  DECIMAL(10,2),
  `OrderDate` DATETIME,
  `Status`    VARCHAR(150) NOT NULL,
  CONSTRAINT `PK_Orders` PRIMARY KEY (`OrderID`),
  CONSTRAINT `FK_Orders_Users` FOREIGN KEY (`UserID`) REFERENCES `Users` (`UserID`) ON DELETE CASCADE
);

/*
** Add table "Order Details"
*/

CREATE TABLE `Order Details` (
  `OrderID`   INTEGER     NOT NULL,
  `ProductID` INTEGER     NOT NULL,
  `Quantity`  SMALLINT(2) NOT NULL DEFAULT 1,
  `Total`     DECIMAL(10,2) NOT NULL,
  CONSTRAINT `PK_Order Details` PRIMARY KEY (`OrderID`, `ProductID`),
  CONSTRAINT `FK_Order_Details_Orders` FOREIGN KEY (`OrderID`) REFERENCES `Orders` (`OrderID`) ON DELETE CASCADE,
  CONSTRAINT `FK_Order_Details_Products` FOREIGN KEY (`ProductID`) REFERENCES `Products` (`ProductID`) ON DELETE CASCADE
);
CREATE TABLE `Messages` (
  `MessageID` INTEGER     NOT NULL AUTO_INCREMENT,
  `FullName`  VARCHAR(50) NOT NULL,
  `Email`     VARCHAR(50) NOT NULL,
  `Subject`   VARCHAR(128),
  `Content`   VARCHAR(158),
  CONSTRAINT `PK_Messages` PRIMARY KEY (`MessageID`)
);

/*
** Add table "Subscribers"
*/

CREATE TABLE `Subscribers` (
  `Email` VARCHAR(50)  NOT NULL
);


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

DROP PROCEDURE IF EXISTS ProfileAddressEdit;
DROP PROCEDURE IF EXISTS CheckoutOrder;


DELIMITER //
CREATE PROCEDURE ProfileAddressEdit(IN req_params_id INT,
                                IN req_body_fullname VARCHAR(50),
                                IN req_body_streetAddress VARCHAR(255),
                                IN req_body_postcode VARCHAR(5),
                                IN req_body_city VARCHAR(28),
                                IN req_body_country VARCHAR(28),
                                IN req_body_phone VARCHAR(12),
                                IN req_body_password VARCHAR(24),
                                IN req_user_password VARCHAR(24),
                                IN bcrypt_nodejs_compareSync_1_output BOOLEAN)
            ProfileAddressEdit_Label:BEGIN
                DECLARE var_s_id INT DEFAULT -1;
                IF (NOT bcrypt_nodejs_compareSync_1_output) THEN
                    LEAVE ProfileAddressEdit_Label;
                END IF;
                SELECT * FROM Addresses WHERE AddressID = req_params_id;
                UPDATE Addresses SET Fullname = req_body_fullname,
                    StreetAddress = req_body_streetAddress,
                    PostCode = req_body_postcode,
                    City = req_body_city,
                    Country = req_body_country,
                    Phone = req_body_phone
                WHERE AddressID = req_params_id;
END//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE CheckoutOrder(
                            IN req_user_UserID INT,
                            IN req_session_address_AddressID INT,
                            IN req_session_cartSummary_subTotal DECIMAL(10, 2),
                            IN req_session_cartSummary_discount DECIMAL(10, 2),
                            IN req_session_cartSummary_shipCost DECIMAL(10, 2),
                            IN req_session_cartSummary_total DECIMAL(10, 2),
                            IN req_session_cart_json VARCHAR(1024)
                            )
            CheckoutOrder_Label:BEGIN
                DECLARE i INT DEFAULT 0;
                DECLARE var_json_key VARCHAR(128);
                DECLARE var_0 VARCHAR(1024);
                DECLARE var_1 INT;
                DECLARE var_2 INT;
                DECLARE var_3 DECIMAL(10, 2);
                DECLARE var_inserted_id_0 INT DEFAULT NULL;
                DECLARE var_json_keys_0 JSON;
                CREATE TEMPORARY TABLE IF NOT EXISTS table_0 LIKE Orders;
                TRUNCATE table_0; 

                INSERT INTO Orders
                    VALUES(null, req_user_UserID, req_session_address_AddressID,
                    req_session_cartSummary_subTotal, req_session_cartSummary_discount,
                    req_session_cartSummary_shipCost, req_session_cartSummary_total, 
                    NOW(), 'Order Received'); 
                SET var_inserted_id_0 := LAST_INSERT_ID();

                SELECT JSON_KEYS(req_session_cart_json) INTO var_json_keys_0;
                WHILE i < JSON_LENGTH(var_json_keys_0) DO
                    SET var_json_key := JSON_EXTRACT(JSON_UNQUOTE(var_json_keys_0), CONCAT('$[', i, ']'));
                    SET var_0 := JSON_EXTRACT(req_session_cart_json, CONCAT("$.", var_json_key));
                    SET var_2 := JSON_EXTRACT(var_0, CONCAT("$.quantity"));
                    IF (var_2 > 0) THEN
                        SET var_1 := JSON_EXTRACT(var_0, "$.ProductID");
                        SET var_3 := JSON_EXTRACT(var_0, "$.productTotal");
                        INSERT INTO \`Order Details\` 
                            VALUES(var_inserted_id_0, var_1, var_2, var_3);
                        UPDATE Products SET UnitsInStock = (UnitsInStock - var_2) 
                            WHERE ProductID = var_1;
                        SET i := i + 1;
                    END IF;
                END WHILE;

                INSERT INTO table_0  
                    SELECT * FROM Orders WHERE OrderID = var_inserted_id_0;

                SELECT * FROM Addresses WHERE AddressID = (SELECT AddressID FROM table_0 LIMIT 1);

                SELECT * FROM \`Order Details\` INNER JOIN (
                        SELECT Products.*, Categories.CategorySlug
                        FROM Products
                        INNER JOIN Categories
                        ON Products.CategoryID = Categories.CategoryID
                    ) \`Table\` ON \`Order Details\`.ProductID = \`Table\`.ProductID
                    WHERE OrderID = (SELECT OrderID FROM table_0 LIMIT 1);
END//
DELIMITER ;



DROP PROCEDURE IF EXISTS UpdateLocation;
DELIMITER //
CREATE PROCEDURE UpdateLocation(IN loc INT,
                                IN var_sub_nbr VARCHAR(15))
            UpdateLocation_Label:BEGIN
                DECLARE var_s_id INT DEFAULT -1;
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
                DECLARE var_s_id INT DEFAULT -1;
                UPDATE subscriber SET bit_1 = var_bit_1 WHERE s_id = var_s_id;
                UPDATE special_facility SET data_a = var_data_a WHERE s_id = var_s_id AND sf_type = var_sf_type;
END//
DELIMITER ;
