import os
import schedule
from datetime import datetime

atual_data = datetime.date(datetime.now())
data_format = atual_data.strftime("%m-%d-%Y")
url = str(f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?%40moeda=%27USD%27&%40dataInicial=%2701-02-2023%27&%40dataFinalCotacao=%27{data_format}%27&%24format=json")
command = str(f"docker exec namenode curl --url '{url}' -o mba-datasets/staging/{atual_data}.json")

def iniciar_carga_hdfs():
    print(url)
    os.system("docker exec namenode mkdir -p /mba-datasets/staging")
    os.system("docker exec namenode mkdir -p /mba-datasets/backup")
    os.system("docker exec namenode hadoop fs -mkdir -p /mba-datasets")
    os.system("docker exec namenode rm /mba-datasets/staging/*.json")
    os.system("docker exec namenode rm /mba-datasets/backup/*.json")
    os.system("docker exec namenode hadoop fs -rm /mba-datasets/*.json")
    os.system(command)
    os.system(f"docker exec namenode hadoop fs -put /mba-datasets/staging/{atual_data}.json /mba-datasets")
    os.system(f"docker exec namenode rm /mba-datasets/staging/{atual_data}.json")
    os.system('docker exec namenode hadoop fs -ls -h -R /mba-datasets | grep -e "^-" | tail -n10')

def job():
    os.system("docker exec namenode find /mba-datasets/backup/ -type f -mtime +7 -delete")
    os.system("docker exec namenode hadoop fs -get /mba-datasets/*.json /mba-datasets/backup")
    os.system("docker exec namenode hadoop fs -rm /mba-datasets/*.json")
    os.system(command)
    os.system(f"docker exec namenode hadoop fs -put /mba-datasets/staging/{atual_data}.json /mba-datasets")
    os.system(f"docker exec namenode rm /mba-datasets/staging/{atual_data}.json")
    os.system('docker exec namenode hadoop fs -ls -h -R /mba-datasets')

if __name__ == '__main__':
    iniciar_carga_hdfs()
    schedule.every().day.at("08:10").do(job)
    while True: 
        schedule.run_pending()
