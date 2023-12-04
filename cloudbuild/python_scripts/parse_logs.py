"""Python Script to parse yarn logs to check of job success.

This python file extracts the status of a dataproc job from its job-id and then
uses this job-id to get the yarn application number.
The yarn application number enables it to read throughh the yarn logs to check
for the number iof records.
In case the number of records match that of BQ table it returns, in case of
mismatch, it throws an error.
"""

from collections.abc import Sequence
import time

from absl import app
from google.cloud import bigquery
from google.cloud import dataproc_v1
from google.cloud import storage


# Remember these are the ones from args.
def get_bq_table_rows(project, dataset, table):
  dataset_ref = bigquery.DatasetReference(project=project, dataset_id=dataset)
  table_ref = bigquery.TableReference(dataset_ref, table_id=table)
  client = bigquery.Client(project=project_id)
  table = client.get_table(table_ref)
  row_count = table.num_rows
  return row_count


def get_blob_as_string(logs_path, bucket, bucket_name, end_file_name):
  blob_name = f"{logs_path.split(bucket_name)[1][1:]}/{end_file_name}"
  # It may be the case that all workers do not produce a log file:
  downloaded_blob = ""
  try:
    blob = bucket.blob(blob_name)
    downloaded_blob = blob.download_as_text(encoding="latin-1")
  except Exception as e:
    print("File Not Found", e)
  return downloaded_blob


def extract_metric(downloaded_blob, metric_string):
  total_metric_sum_in_blob = 0
  idx = downloaded_blob.find(metric_string)
  # Keep on finding the metric value as there can be
  # 1 or more outputs in a log file.
  while idx != -1:
    records_read = downloaded_blob[idx:].split("\n")[0].split(metric_string)[1]
    total_metric_sum_in_blob += int(records_read.strip())
    # Now search in the remaining file.
    idx = downloaded_blob.find(metric_string, idx + 1)
  return total_metric_sum_in_blob


def get_blob_and_check_metric(
    logs_path, bucket, bucket_name, end_file_name, metric_string
):
  downloaded_blob = get_blob_as_string(
      logs_path, bucket, bucket_name, end_file_name
  )
  if metric_string in downloaded_blob:
    metric_value = extract_metric(downloaded_blob, metric_string)
    return True, metric_value
  return False


def get_job_path_state(project_id, region, job_id):
  """Method to return dataproc job state and yarn logs path.

  Args:
    project_id: Project Id of the GCP project that contains the cluster.
    region: region in whcih the cluster runs.
    job_id: JOB_Id of the dataproc job.

  Returns:

  Raises:
    RuntimeError: In case the Dataproc job fails
  """
  # Create a client
  client = dataproc_v1.JobControllerClient(
      client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
  )

  # Initialize request argument(s)
  request = dataproc_v1.GetJobRequest(
      project_id=project_id,
      region=region,
      job_id=job_id,
  )

  # Make the request
  response = client.get_job(request=request)
  state = response.status.state.name

  if state == "ERROR":
    raise RuntimeError("Job Failed")

  cluster_id = response.placement.cluster_uuid

  tracking_url = response.yarn_applications[0].tracking_url
  if tracking_url.endswith("/"):
    tracking_url = tracking_url[:-1]
  yarn_application_number = tracking_url.split("/")[-1]
  yarn_job_number = yarn_application_number.split("_")[-1]

  logs_path = f"gs://dataproc-temp-{region}-893516231318-84gvkikz/{cluster_id}/yarn-logs/root/bucket-logs-ifile/{yarn_job_number}/{yarn_application_number}"
  return logs_path, state


def discover_read_return(logs_path, project, log_names):
  print("GCS Link", logs_path)
  metric = read_logs(logs_path, project, log_names)
  return metric


def read_logs(logs_path, project, log_names):
  """Method to parse the number of records from the yarn logs.

  Args:
    logs_path: location of GCP bucket where yarn logs are stored
    project: Project Id of the GCP project that contains the cluster.
    log_names: Name of the log files inside the logs_path bucket.

  Returns:

  Raises:
    FileNotFoundError: In case file does not exist at the specified location.
  """
  bucket_name = logs_path.split("/")[2]
  storage_client = storage.Client(project=project)
  bucket = storage_client.bucket(bucket_name)
  metric_string = "Number of records read: "
  # Sum across all the worker files.
  total_metric_count = 0
  # Check if metric is present in atleast one of the files.
  is_metric_found = False
  for log_name in log_names:
    is_metric_present = get_blob_and_check_metric(
        logs_path, bucket, bucket_name, log_name, metric_string
    )
    if isinstance(is_metric_present, tuple):
      is_metric_found = True
      # Sum up all the values.
      total_metric_count += is_metric_present[1]
  # If found, return the value, else raise an error.
  if(is_metric_found):
    return total_metric_count
  raise FileNotFoundError("Error parsing the yarn logs..")


def run(
    project_id, log_names, region, job_id, arg_project, arg_dataset, arg_table
):
  """Method that calls all the heloper function to determine success of a job.

  Args:
    project_id: Project Id of the GCP project that contains the cluster.
    log_names: Paths of the respective worker log files
    region: region in whcih the cluster runs.
    job_id: JOB_Id of the dataproc job.
    arg_project: Resource project id (from which rows are read)
    arg_dataset: Resource dataset name (from which rows are read)
    arg_table: Resource table name (from which rows are read)

  Raises:
    RuntimeError: In case the dataproc job fails
    AssertionError: When the rows read by connector and in the BQ table do not
    match.
  """
  # This works for the perfect case:
  start_time = time.time()
  logs_path, state = get_job_path_state(project_id, region, job_id)
  print("Time discover_params():", time.time() - start_time, "s")
  if state == "ERROR":
    raise RuntimeError("Job Failed")
  start_time = time.time()
  metric = discover_read_return(logs_path, project_id, log_names)
  print("Time discover_read_return():", time.time() - start_time, "s")
  start_time = time.time()
  # Uncomment after access has been provided.
  bq_table_rows = get_bq_table_rows(arg_project, arg_dataset, arg_table)
  # bq_table_rows = 33294
  print("Time get_bq_table_rows():", time.time() - start_time, "s")
  if metric != bq_table_rows:
    raise AssertionError("Rows do not match")


def main(argv: Sequence[str]) -> None:
  if len(argv) != 9:
      raise app.UsageError("Too many or too less command-line arguments.")
    
  # Defining default values.
  job_id = ""
  project_id = ""
  cluster_name = ""
  no_workers = 0
  region = ""
  arg_project = ""
  arg_dataset = ""
  arg_table = ""

  # Getting the arguments one by one.
  for argument in argv[1:]:
    arg_value = argument.split("=")
    arg_value = arg_value[1]
    print("argument", argument)  
    print("arg_value", arg_value)  
    if argument.startswith('--job_id'):
      job_id = arg_value
    elif argument.startswith("--project_id"):
      project_id = arg_value
    elif argument.startswith("--cluster_name"):
      cluster_name = arg_value
    elif argument.startswith("--no_workers"):
      no_workers = int(arg_value)
    elif argument.startswith("--region"):
      region = arg_value
    elif argument.startswith("--arg_project"):
      arg_project = arg_value
    elif argument.startswith("--arg_dataset"):
      arg_dataset = arg_value
    elif argument.startswith("--arg_table"):
      arg_table = arg_value
    else:
      raise UserWarning("Invalid argument provided")

  # Check if all arguments are there.
  if job_id == "":
    raise UserWarning("job_id argument not provided")
  elif project_id == "":
    raise UserWarning("project_id argument not provided")
  elif cluster_name == "":
    raise UserWarning("cluster_name argument not provided")
  elif no_workers == 0:
    raise UserWarning("no_workers argument not provided")
  elif region == "":
    raise UserWarning("region argument not provided")
  elif arg_project == "":
    raise UserWarning("arg_project argument not provided")
  elif arg_dataset == "":
    raise UserWarning("arg_dataset argument not provided")
  elif arg_table == "":
    raise UserWarning("arg_table argument not provided")
        
  log_names = []
  for worker_no in range(no_workers):
    log_names.append(
        f"{cluster_name}-w-{worker_no}.{region}-c.c.{project_id}.internal_8026"
    )

  run(
      project_id, log_names, region, job_id, arg_project, arg_dataset, arg_table
  )

if __name__ == "__main__":
  app.run(main)
