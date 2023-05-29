from datetime import datetime, timedelta
import random
import json
import boto3

STREAM_NAME = "parcial3"



def get_data():
    ini = datetime(2022, 5, 1)
    fin = datetime(2023, 5, 1)

    diff = fin - ini

    diasAleatorios = random.randint(0, diff.total_seconds())

    fecha= ini + timedelta(seconds=diasAleatorios)
    dolar= round(random.uniform(4000, 5000))
    return {
        'date': fecha.strftime('%Y-%m-%d'),
        'close': dolar}

def generate(stream_name, kinesis_client):
    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partitionkey")


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))