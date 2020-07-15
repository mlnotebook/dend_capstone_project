s3_table_keys = [
  {'name': 'public.staging_immigration',
   'key': 'sas_data',
   'file_format': 'parquet',
   'sep': ''
  },
  {'name': 'public.staging_airports',
   'key': 'airport/airport-codes_csv.csv',
   'file_format': 'csv',
   'sep': ','
  },
  {'name': 'public.staging_temperature',
   'key': 'temperature/GlobalLandTemperaturesByCity.csv',
   'file_format': 'csv',
   'sep': ','
  },
  {'name': 'public.staging_demographics',
   'key': 'demographics/us-cities-demographics.csv',
   'file_format': 'csv',
   'sep': ';'
  }
]


sas_table_configs = [
  {'name': 'dim_i94cit',
   'value': 'i94cntyl',
   'columns': ['code', 'country'],
   'qc_check': [{'qc_sql': "SELECT COUNT(*) FROM public.dim_i94cit WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'dim_i94port',
   'value': 'i94prtl',
   'columns': ['code', 'port'],
   'qc_check': [{'qc_sql': "SELECT COUNT(*) FROM public.dim_i94port WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'dim_i94mode',
   'value': 'i94model',
   'columns': ['code', 'mode'],
   'qc_check': [{'qc_sql': "SELECT COUNT(*) FROM public.dim_i94mode WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'dim_i94addr',
   'value': 'i94addrl',
   'columns': ['code', 'addr'],
   'qc_check': [{'qc_sql': "SELECT COUNT(*) FROM public.dim_i94addr WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'dim_i94visa',
   'value': 'I94VISA',
   'columns': ['code', 'type'],
   'qc_check': [{'qc_sql': "SELECT COUNT(*) FROM public.dim_i94visa WHERE code is null", 'expected_result': 0}]
  }
]
