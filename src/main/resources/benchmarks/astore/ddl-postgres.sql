
DROP TABLE IF EXISTS ProfileIdEdit CASCADE;
DROP TABLE IF EXISTS ProfileIdChangePassword CASCADE;
DROP TABLE IF EXISTS ProfileAddressAdd CASCADE;
DROP TABLE IF EXISTS ProfileAddressDelete CASCADE;
DROP TABLE IF EXISTS ProfileAddressEdit CASCADE;
DROP TABLE IF EXISTS RoutesSubscribe CASCADE;
DROP TABLE IF EXISTS CheckoutDeliveryNew CASCADE;
DROP TABLE IF EXISTS CheckoutOrder CASCADE;
DROP TABLE IF EXISTS ContactUs CASCADE;
DROP TABLE IF EXISTS Messages CASCADE;
DROP TABLE IF EXISTS Subscribers CASCADE;

CREATE TABLE "Categories" (
  "CategoryID"   SERIAL    NOT NULL,
  "CategoryName" VARCHAR(58) NOT NULL,
  "Description"  VARCHAR(300),
  "CategorySlug" VARCHAR(68) NOT NULL,
  "Image"        VARCHAR(58) NOT NULL,
  CONSTRAINT "PK_Categories" PRIMARY KEY ("CategoryID")
);

CREATE INDEX "CategoryName" ON "Categories" ("CategoryName");

CREATE TABLE "Users" (
  "UserID"        SERIAL   NOT NULL,
  "FullName"      VARCHAR(50)  NOT NULL,
  "StreetAddress" VARCHAR(255) NOT NULL,
  "PostCode"      VARCHAR(5)   NOT NULL,
  "City"          VARCHAR(28)  NOT NULL,
  "Country"       VARCHAR(28)  NOT NULL,
  "Phone"         VARCHAR(12)  NOT NULL,
  "Email"         VARCHAR(50)  NOT NULL,
  "Username"      VARCHAR(28),
  "Password"      VARCHAR(158),
  "Admin"         BOOLEAN      NOT NULL DEFAULT FALSE,
  CONSTRAINT "PK_Users" PRIMARY KEY ("UserID")
);

CREATE INDEX "Username" ON "Users" ("Username");

CREATE TABLE "Addresses" (
  "AddressID"     SERIAL      NOT NULL,
  "UserID"        INTEGER,
  "FullName"      VARCHAR(50)  NOT NULL,
  "StreetAddress" VARCHAR(255) NOT NULL,
  "PostCode"      VARCHAR(5)   NOT NULL,
  "City"          VARCHAR(28)  NOT NULL,
  "Country"       VARCHAR(28)  NOT NULL,
  "Phone"         VARCHAR(12)  NOT NULL,
  CONSTRAINT "PK_Addresses" PRIMARY KEY ("AddressID"),
  CONSTRAINT "FK_Users_UserID" FOREIGN KEY ("UserID") REFERENCES "Users" ("UserID") ON DELETE CASCADE
);

/*
** Add table "Products"
*/

CREATE TABLE "Products" (
  "ProductID"       SERIAL      NOT NULL,
  "ProductName"     VARCHAR(40)  NOT NULL,
  "CategoryID"      INTEGER,
  "ProductPrice"    DECIMAL(10, 2)        DEFAULT 0,
  "UnitsInStock"    INT          DEFAULT 0,
  "Description"     VARCHAR(255) NOT NULL,
  "ManufactureYear" INT  NOT NULL,
  "Image"           VARCHAR(50)  NOT NULL,
  "ProductSlug"     VARCHAR(50)  NOT NULL,
  "Feature"         BOOLEAN      NOT NULL DEFAULT False,
  CONSTRAINT "PK_Products" PRIMARY KEY ("ProductID"),
  CONSTRAINT "FK_Products_Categories" FOREIGN KEY ("CategoryID") REFERENCES "Categories" ("CategoryID") ON DELETE CASCADE
);

CREATE INDEX "ProductName" ON "Products" ("ProductName");

CREATE TABLE "Orders" (
  "OrderID"   SERIAL NOT NULL,
  "UserID"    INTEGER NOT NULL,
  "AddressID" INTEGER NOT NULL,
  "SubTotal"  DECIMAL(10,2),
  "Discount"  DECIMAL(10,2),
  "ShippingFee"  DECIMAL(10,2),
  "Total"  DECIMAL(10,2),
  "OrderDate" TIMESTAMP,
  "Status"    VARCHAR(150) NOT NULL,
  CONSTRAINT "PK_Orders" PRIMARY KEY ("OrderID"),
  CONSTRAINT "FK_Orders_Users" FOREIGN KEY ("UserID") REFERENCES "Users" ("UserID") ON DELETE CASCADE
);

/*
** Add table "Order Details"
*/

CREATE TABLE "Order Details" (
  "OrderID"   INTEGER     NOT NULL,
  "ProductID" INTEGER     NOT NULL,
  "Quantity"  INT NOT NULL DEFAULT 1,
  "Total"     DECIMAL(10,2) NOT NULL,
  CONSTRAINT "PK_Order Details" PRIMARY KEY ("OrderID", "ProductID"),
  CONSTRAINT "FK_Order_Details_Orders" FOREIGN KEY ("OrderID") REFERENCES "Orders" ("OrderID") ON DELETE CASCADE,
  CONSTRAINT "FK_Order_Details_Products" FOREIGN KEY ("ProductID") REFERENCES "Products" ("ProductID") ON DELETE CASCADE
);
CREATE TABLE "Messages" (
  "MessageID" SERIAL     NOT NULL,
  "FullName"  VARCHAR(50) NOT NULL,
  "Email"     VARCHAR(50) NOT NULL,
  "Subject"   VARCHAR(128),
  "Content"   VARCHAR(158),
  CONSTRAINT "PK_Messages" PRIMARY KEY ("MessageID")
);

/*
** Add table "Subscribers"
*/

CREATE TABLE "Subscribers" (
  "Email" VARCHAR(50)  NOT NULL
);



