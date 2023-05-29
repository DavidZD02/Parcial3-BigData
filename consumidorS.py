import boto3
import logging
from botocore.exceptions import ClientError
import statistics

dolar = []


def process_records(records):
    global dolar
    for record in records:
        data = (record['Data'])
        datas=data.decode('utf-8')
        dic = eval(datas)
        dolarr = dic['close']

        dolar.append(dolarr) 

        if len(dolar) >= 21:
            dolar_= dolar[:-1]
            bollingerr = bollinger(dolar_)
            print("Precio",dolarr)
            print("Bollinger",bollingerr)
            if dolarr > bollingerr:
                print("El precio está por encima de la franja superior")
            dolar = dolar[-20:]

    return dolarr


def bollinger(dol):
    bolling = None
    if  isinstance(dol, list) and len(dol) >= 20:
        std = statistics.stdev(dol[-20:])
        media = sum(dol[-20:]) / len(dol[-20:])
        bolling = media+ (2 * std)

    return bolling



def main():
    stream_name = 'parcial3'

    try:
        kinesis_client = boto3.client('kinesis')

        response = kinesis_client.describe_stream(StreamName=stream_name)
        shard_id = response['StreamDescription']['Shards'][3]['ShardId']

        response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
        shard_iterator = response['ShardIterator']
        max_records = 100
        record_count = 0

        while record_count < max_records:
            response = kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=1
            )

            shard_iterator = response['NextShardIterator']
            records = response['Records']
            record_count += len(records)
            process_records(records)
            try:
                print(records[0]["Data"])
            except IndexError:
                pass

    except ClientError:
        logger = logging.getLogger()
        logger.exception("Couldn't get records from stream %s.", stream_name)
        raise

if __name__ == "__main__":
    main()