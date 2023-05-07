import os
from pyhive import hive

def iniciar_carga_hdfs():
    print("Inicio")
    
    print("=== Cria diretorios para transferir arquivos ====")
    os.system("sudo docker exec namenode mkdir trabtwoddps")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/address")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/customer")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/customeraddress")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/product")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/productcategory")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/productdescription")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/productmodel")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/productmodeldescription")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/salesorderdetail")
    os.system("sudo docker exec namenode hadoop fs -mkdir /app/trabalho/hive/salesorderderhead")
    
    print("=== Povoar diretorios HDFS ====")
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1xiR-JmKVcL2r4EJkrJ-mqd8HInLdqasa' -o trabtwoddps/address.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/address/address.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/address")
    
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1j0i4sGBuY996uC8qcS8ZZvLTBN2YqK6a' -o trabtwoddps/customer.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/customer/customer.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/customer.csv /app/trabalho/hive/customer")
    
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=18YskJR7QaZPPbt6uBNN48eGQNYbUYGpB' -o trabtwoddps/customeraddress.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/customeraddress/customeraddress.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/customeraddress.csv /app/trabalho/hive/customeraddress")

    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=11bDt7PGrVfdhJZRIeOLgrqLUXf2opUqF' -o trabtwoddps/product.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/product/product.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/product.csv /app/trabalho/hive/product")
    
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1AKgHJ1InA7YAAFwQNGhwY29kiL34L7bc' -o trabtwoddps/productcategory.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/productcategory/productcategory.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/productcategory.csv /app/trabalho/hive/productcategory")

    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1Ybxo4GB7v73d1qacgVtCwnfSaQkQCOxS' -o trabtwoddps/productdescription.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/productdescription/productdescription.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/productdescription.csv /app/trabalho/hive/productdescription")
    
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1EwXN_iNohdnpgw-CogzeNNXpFp_ZwwYu' -o trabtwoddps/productmodel.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/productmodel/productmodel.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/productmodel.csv /app/trabalho/hive/productmodel")

    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1x6GuS7pq2M_pqv1s0CIP6qiGXZaEjeZd' -o trabtwoddps/productmodeldescription.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/productmodeldescription/productmodeldescription.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/productmodeldescription.csv /app/trabalho/hive/productmodeldescription")
    
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1ChW5hfwmD4l6llZIeLesvVZLUr9BnGu9' -o trabtwoddps/salesorderdetail.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/salesorderdetail/salesorderdetail.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/salesorderdetail.csv /app/trabalho/hive/salesorderdetail")
    
    os.system("sudo docker exec namenode curl -Lb ./cookie 'https://drive.google.com/uc?export=download&id=1K0uYlD9En3hP2u7e1cCLgF_gBydilDWI' -o trabtwoddps/salesorderderhead.csv")
    os.system("sudo docker exec namenode hadoop fs -rm /app/trabalho/hive/salesorderderhead/salesorderderhead.csv")
    os.system("sudo docker exec namenode hadoop fs -put /trabtwoddps/salesorderderhead.csv /app/trabalho/hive/salesorderderhead")
    
    os.system("sudo docker exec namenode rm -rf trabtwoddps/")

    print("=== Cria DataBase AdventureWorks ====")
    conn = hive.Connection(host="localhost", port=10000, username="hive", password="hive", database="default", auth='CUSTOM')
    cursor = conn.cursor()
    print(cursor.fetchall())
    cursor.execute("create database if not exists AdventureWorks location '/app/trabalho/hive'")
    cursor.execute("show databases")
    conn.close()
    
    print("=== Cria Tabelas em AdventureWorks ====")
    conn = hive.Connection(host="localhost", username="", password="", database="adventureworks", auth='CUSTOM')
    cursor = conn.cursor()
    cursor.execute("create external table address (AddressID bigint, AddressLine1 string, AddressLine2 string, City string, PostalCode string, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/address' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table customer (CustomerID bigint, Title string, Suffix string, CompanyName string, SalesPerson string,	EmailAddress string, PasswordHash string, PasswordSalt	string, rowguid string,	ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/customer' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table customeraddress (CustomerID bigint, AddressID bigint, rowguid string,	ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/customeraddress' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table product (ProductID bigint, ProductNumber string, Color string, StandardCost double, ListPrice double, Size bigint, Weight double, ProductCategoryID bigint, ProductModelID bigint, SellStartDate string, SellEndDate string, DiscontinuedDate string, ThumbNailPhoto string, ThumbnailPhotoFileName string, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/product' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table productcategory (ProductCategoryID bigint, ParentProductCategoryID bigint, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/productcategory' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table productdescription (ProductDescriptionID bigint, Description string, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/productdescription' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table productmodel (ProductModelID bigint, CatalogDescription string, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/productmodel' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table productmodeldescription (ProductModelID bigint, ProductDescriptionID bigint, Culture string, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/productmodeldescription' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table salesorderdetail (SalesOrderID bigint, SalesOrderDetailID bigint, OrderQty int, ProductID bigint, UnitPrice double, UnitPriceDiscount double, LineTotal double, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/salesorderdetail' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("create external table salesorderderhead (SalesOrderID bigint, RevisionNumber int, OrderDate string, DueDate string, ShipDate string, Status int, SalesOrderNumber string, CustomerID bigint, ShipToAddressID bigint, BillToAddressID bigint, ShipMethod string, CreditCardApprovalCode int, SubTotal double, TaxAmt double, Freight double, TotalDue double, Comment string, rowguid string, ModifiedDate string) row format delimited fields terminated by ';' stored as textfile location '/app/trabalho/hive/salesorderderhead' tblproperties ('skip.header.line.count'='1')")
    cursor.execute("show tables")
    print(cursor.fetchall())
    print("Fim")

    # select c.customerid, sh.salesorderid from adventureworks.customer c join
    # adventureworks.salesorderderhead sh on
    # c.customerid = sh.customerid
    # where sh.customerid is not null;

    # select sd.salesorderdetailid, sd.salesorderid, p.productid
    # from adventureworks.product p join
    # adventureworks.salesorderdetail sd
    # on sd.productid = p.productid
    # where sd.productid is not null;


if __name__ == '__main__':
    iniciar_carga_hdfs()