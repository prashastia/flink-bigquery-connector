"""Python script for BQ Table data append.

Python script to add partitions to a BigQuery partitioned table.
"""
from collections.abc import Sequence
import datetime
import threading
import time

from absl import app

from utils import tableType
from utils import utils


def wait():
    print(
        'Going to sleep, waiting for connector to read existing, Time:'
        f' {datetime.datetime.now()}'
    )
    # This is the time connector takes to read the previous rows
    time.sleep(2.5 * 60)
    return


def main(argv: Sequence[str]) -> None:
    if len(argv) != 6:
        raise app.UsageError('Too many or too les command-line arguments.')

    now_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    project_id = ''
    dataset_id = ''
    table_id = ''
    refresh_time = 10
    for argument in argv[1:]:
        arg_value = argument.split('=')
        arg_value = arg_value[1]
        if argument.startswith('--now_timestamp'):
            now_timestamp = datetime.datetime.strptime(
                arg_value, '%Y-%m-%d'
            ).astimezone(datetime.timezone.utc)
        elif argument.startswith('--arg_project'):
            project_id = arg_value
        elif argument.startswith('--arg_dataset'):
            dataset_id = arg_value
        elif argument.startswith('--arg_table'):
            table_id = arg_value
        elif argument.startswith('--refresh_interval'):
            refresh_time = int(arg_value)
        else:
            raise UserWarning('Invalid argument provided')

    if not project_id:
        raise UserWarning('project_id argument not provided')
    elif not dataset_id:
        raise UserWarning('dataset_id argument not provided')
    elif not table_id:
        raise UserWarning('table_id argument not provided')

    # 1. Create the partioned table.
    table_id = f'{project_id}.{dataset_id}.{table_id}'

    # 2. Now add the partitions to the table.
    simple_avro_schema_fields_string = (
        '"fields": [{"name": "name", "type": "string"},{"name": "number",'
        '"type": "long"},{"name" : "ts", "type" : {"type" :'
        '"long","logicalType": "timestamp-micros"}}]'
    )
    simple_avro_schema_string = (
        '{"namespace": "project.dataset","type": "record","name":'
        ' "table","doc": "Avro Schema for project.dataset.table",'
        + simple_avro_schema_fields_string
        + '}'
    )
    partitions = [2, 1, 2]
    no_rows_per_partition = 10
    no_batches = 10
    no_rows_per_batch = int(no_rows_per_partition / no_batches)

    table_type = tableType.PartitionedTable(now_timestamp)
    avro_file_local = 'mockData.avro'
    table_creation_utils = utils.TableCreationUtils(
        simple_avro_schema_string,
        table_type,
        avro_file_local,
        no_rows_per_batch,
        table_id,
    )
    # TODO: Set this to be equal to the previous number of partitions.
    prev_partitions_offset = 3
    for no_partitions in partitions:
        start_time = time.time()
        prev_partitions_offset += 1
        # Wait for the connector to read previous inserted rows.
        wait()
        total_rows = 0
        print(f'Begin Inserting, Time: {datetime.datetime.now()}')

        for partition_no in range(no_partitions):
            threads = list()
            total_rows += (no_batches*no_rows_per_batch)
            for i in range(no_batches):
                x = threading.Thread(
                    target=table_creation_utils.create_transfer_records,
                    kwargs={
                        'thread_no': str(i),
                        'partition_no': partition_no + prev_partitions_offset,
                    },
                )
                threads.append(x)
                x.start()
            for _, thread in enumerate(threads):
                thread.join()
            print(f'Completed writing Partition {partition_no}')

        print(f'Finished Inserting, Time: {datetime.datetime.now()}')
        time_elapsed = time.time() - start_time
        print(f'Write Time: {(time_elapsed)}s for {total_rows} rows')
        print(
            'Throughput:'
            f' {round((no_rows_per_batch * no_batches) / time_elapsed)} rows/sec'
        )
        prev_partitions_offset += no_partitions
        # We wait for the refresh to happen
        # so that the data just created can be read.
        print(
            'Waiting for the new read to complete,\nWill add more data after'
            f' refresh has finished.\nTime: {datetime.datetime.now()}'
        )
        while time_elapsed < float(60 * refresh_time):
            time_elapsed = time.time() - start_time
    # Wait for the final read to happen.
    wait()
    print(f'Ending, Time: {datetime.datetime.now()}')


if __name__ == '__main__':
    app.run(main)
