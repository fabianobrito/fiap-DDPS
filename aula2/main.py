import os

def iniciar_carga_hdfs():
    print("Inicio")
    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1xiR-JmKVcL2r4EJkrJ-mqd8HInLdqasa/view?usp=share_link -o trabtwoddps/address.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/address")
    os.system("docker exec rm trabtwoddps/address.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1j0i4sGBuY996uC8qcS8ZZvLTBN2YqK6a/view?usp=share_link -o trabtwoddps/customer.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/customer")
    os.system("docker exec rm /trabtwoddps/customer.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/18YskJR7QaZPPbt6uBNN48eGQNYbUYGpB/view?usp=share_link -o trabtwoddps/customeraddress.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/customeraddress")
    os.system("docker exec rm /trabtwoddps/customeraddress.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/18YskJR7QaZPPbt6uBNN48eGQNYbUYGpB/view?usp=share_link -o trabtwoddps/product.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/product")
    os.system("docker exec rm /trabtwoddps/product.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1AKgHJ1InA7YAAFwQNGhwY29kiL34L7bc/view?usp=share_link -o trabtwoddps/productcategory.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/productcategory")
    os.system("docker exec rm /trabtwoddps/productcategory.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1Ybxo4GB7v73d1qacgVtCwnfSaQkQCOxS/view?usp=share_link -o trabtwoddps/productdescription.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/productdescription")
    os.system("docker exec rm /trabtwoddps/productdescription.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1EwXN_iNohdnpgw-CogzeNNXpFp_ZwwYu/view?usp=share_link -o trabtwoddps/productmodel.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/productmodel")
    os.system("docker exec rm /trabtwoddps/productmodel.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1x6GuS7pq2M_pqv1s0CIP6qiGXZaEjeZd/view?usp=share_link -o trabtwoddps/productmodeldescription.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/productmodeldescription")
    os.system("docker exec rm /trabtwoddps/productmodeldescription.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1ChW5hfwmD4l6llZIeLesvVZLUr9BnGu9/view?usp=share_link -o trabtwoddps/salesorderdetail.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/salesorderdetail")
    os.system("docker exec rm /trabtwoddps/salesorderdetail.csv")

    os.system("docker exec namenode curl --url https://drive.google.com/file/d/1K0uYlD9En3hP2u7e1cCLgF_gBydilDWI/view?usp=share_link -o trabtwoddps/salesorderderhead.csv")
    os.system("docker exec namenode hadoop fs -put /trabtwoddps/address.csv /app/trabalho/hive/salesorderderhead")
    os.system("docker exec rm /trabtwoddps/salesorderderhead.csv")
    print("Fim")

if __name__ == '__main__':
    iniciar_carga_hdfs()