import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions

QUERY = """
SELECT unique_key, trip_start_timestamp, trip_miles, fare, tips, payment_type
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_start_timestamp >= '2022-01-01' AND trip_start_timestamp < '2022-01-02'
LIMIT 20
"""

def run():

    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(query=QUERY, use_standard_sql=True)    # load 20 records from the table, returns list of dicts [{"col": val, "col2": val}]
            | "KeepCreditCard"   >> beam.Filter(lambda row: row["payment_type"] == "Credit Card")   # filter records with credit card payments
            | "AddTipPct"        >> beam.Map(lambda row: {**row, "tip_pct": round(row["tips"]/row["fare"], 3) if row["fare"] else None})  # create new col with tips %
            | "Print"            >> beam.Map(print)   # print the table                                                                                  
        )

if __name__ == "__main__":
    run()
