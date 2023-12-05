"""Abstract class for table type.

We can have other classses implementing this class.
"""

import abc
import datetime
import random
import string
import sys


class TableType(abc.ABC):
  """Abstract class for table type.

  We can have other classses implementing this class.
  """

  def generate_entries(self, no_elements_in_array):
    # Do not exceed more than this, BQ limit exceeds.
    return [sys.maxsize] * no_elements_in_array  # Array of LONGS: entries_i

  def generate_record(self, no_levels, level_no=0):
    if level_no == no_levels:
      return {"level_" + str(level_no): {"value": self.generate_long()}}
    return {
        "level_" + str(level_no): self.generate_record(no_levels, level_no + 1)
    }

  def generate_string(self):
    return "".join(
        random.choices(string.ascii_letters, k=random.randint(10, 1000))
    )

  def generate_long(self):
    return random.choice(range(0, 10000000))

  def is_perfect_hour(self, datetime_obj):
    """Returns True if the datetime object is a perfect hour, False otherwise."""
    return (
        datetime_obj.minute == 0
        and datetime_obj.second == 0
        and datetime_obj.microsecond == 0
    )

  def generate_timestamp(self, current_timestamp):
    """Method to generate a random datetime within the given hour.

    Args:
      current_timestamp: Date is generated within one hour of this timestamp.

    Returns:
      datetime in long format.
    """
    next_hour = current_timestamp + datetime.timedelta(hours=1)
    random_timestamp = random.randint(
        int(current_timestamp.timestamp()), int(next_hour.timestamp())
    )
    utc = datetime.timezone.utc
    random_timestamp_utc = datetime.datetime.fromtimestamp(
        random_timestamp, utc
    )
    # Check if the generated entry is a perfect hour.
    # Note it is only for the case of hour pased partitioning.
    while self.is_perfect_hour(random_timestamp_utc):
      # Keep on regenrating.
      random_timestamp = random.randint(
          int(current_timestamp.timestamp()), int(next_hour.timestamp())
      )
      utc = datetime.timezone.utc
      random_timestamp_utc = datetime.datetime.fromtimestamp(
          random_timestamp, utc
      )
    # BQ reads the data in micro seconds by default.
    # there is loss in precision here.
    in_micros = datetime.datetime.timestamp(random_timestamp_utc) * (10**6)
    # print(in_micros)
    return int(in_micros)

  @abc.abstractmethod
  def write_rows(
      self,
      no_rows_per_batch,
      writer,
      partition_no,
      current_timestamp,
  ):
    pass


class LargeTable(TableType):
  """Inherits abstract class table_type.

  Overwrites the method write_rows() to add rows
  according to the specific table type: Bounded Table of O(GB) order
  """

  # overriding abstract method
  def write_rows(
      self,
      no_rows_per_batch,
      writer,
      partition_no,
      current_timestamp,
  ):
    for _ in range(no_rows_per_batch):
      writer.append({
          "name": self.generate_string(),
          "number": self.generate_long(),
          "ts": self.generate_timestamp(current_timestamp=current_timestamp),
      })


class LargeRowTable(TableType):
  """Inherits abstract class table_type.

  Overwrites the method write_rows() to add rows
  according to the specific table type: Bounded Table with rows O(MB) order
  """

  def __init__(self, no_columns, no_elements_in_array):
    self.no_columns = no_columns
    self.no_elements_in_array = no_elements_in_array

  # overriding abstract method
  def write_rows(
      self,
      no_rows_per_batch,
      writer,
      partition_no,
      current_timestamp,
  ):
    for _ in range(no_rows_per_batch):
      record = {
          "name": self.generate_string(),
          "number": self.generate_long(),
          "ts": self.generate_timestamp(current_timestamp),
      }
      for col_number in range(self.no_columns):
        record[f"entries_{col_number}"] = self.generate_entries(
            self.no_elements_in_array
        )
      writer.append(record)


class NestedSchemaTable(TableType):
  """Inherits abstract class table_type.

  Overwrites the method write_rows() to add rows
  according to the specific table type: Bounded Table with 15 levels
  """

  def __init__(self, no_levels):
    self.no_levels = no_levels

  # overriding abstract method
  def write_rows(
      self,
      no_rows_per_batch,
      writer,
      partition_no,
      current_timestamp,
  ):
    for _ in range(no_rows_per_batch):
      record = self.generate_record(self.no_levels)
      writer.append(record)


class PartitionedTable(TableType):
  """Inherits abstract class table_type.

  Overwrites the method write_rows() to add rows
  according to the specific table type: Partitioned Table
  """

  def __init__(self, now_timestamp):
    self.now_timestamp = now_timestamp

  # overriding abstract method
  def write_rows(
      self,
      no_rows_per_batch,
      writer,
      partition_no,
      current_timestamp,
  ):
    current_timestamp = self.now_timestamp + datetime.timedelta(
        hours=partition_no
    )

    # Write the specified number of rows.
    for _ in range(no_rows_per_batch):
      writer.append({
          "name": self.generate_string(),
          "number": self.generate_long(),
          "ts": self.generate_timestamp(current_timestamp),
      })
