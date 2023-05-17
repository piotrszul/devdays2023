# Databricks notebook source
dbutils.widgets.text('FHIR_ENDPOINT_URL', 'https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir')
dbutils.widgets.text('DESTINATION_URL', 'dbfs:/tmp/fhir-export-01')
dbutils.widgets.text('EXPORT_RESOURCES', 'Patient,Condition,Observation,Immunization')

# COMMAND ----------

FHIR_ENDPOINT_URL = dbutils.widgets.get('FHIR_ENDPOINT_URL')
DESTINATION_URL = dbutils.widgets.get('DESTINATION_URL')
EXPORT_RESOURCES = dbutils.widgets.get('EXPORT_RESOURCES').split(',')

print(f"""Exporting 
 resources: {EXPORT_RESOURCES}
 from: `{FHIR_ENDPOINT_URL}`
 to: `{DESTINATION_URL}`
""")


# COMMAND ----------

import requests
import time
import urllib
from os import path


def bulk_export(fhir_url, resources):
    """
    """
    url = path.join(fhir_url, '$export')
    # Set the required headers for the FHIR bulk export API
    headers = {
        'Accept-Encoding': 'gzip',
        'Accept': 'application/fhir+json',
        'Content-Type': 'application/fhir+json',
        'Prefer': 'respond-async'
    }

    # Set the export parameters for a system-level export
    params = {
        '_outputFormat': 'ndjson',
        '_type': ",".join(resources),
    #    '_since': '2022-01-01T00:00:00Z'
    }

    # Make the HTTP request to the FHIR bulk export API
    response = requests.get(url, headers=headers, params=params)

    # Check the response status code
    if response.status_code == 202:
        # The request was accepted, so retrieve the Content-Location header to poll for the response
        content_location = response.headers['Content-Location']
        print(f'Export request accepted. Polling for response at {content_location}...\n')

        # Poll for the response until the export is complete
        while True:
            time.sleep(5)
            poll_response = requests.get(content_location)

            # Check the response status code
            if poll_response.status_code == 202:
                # The export is still in progress, so wait for a few seconds before polling again
                continue
            elif poll_response.status_code == 200:
                # The export is complete, so retrieve the JSON body containing the URLs to access the exported files
                return poll_response.json()
            else:
                # The request failed
                raise(Exception(f'Error in pool request: {poll_response.status_code}'))
    else:
        # The request failed
        raise Exception(f'Error in kick-off request: {response.status_code}')


# COMMAND ----------

export_response = bulk_export(FHIR_ENDPOINT_URL, EXPORT_RESOURCES)
print(f"""Server side export sucessful with response:
{export_response}""")

# COMMAND ----------

from tempfile import TemporaryDirectory

def dowload_export(export_response, destination_url):
    output = export_response.get('output', [])
    index = 0
    with TemporaryDirectory() as tmpdirname:
        for entry in output:
            input_url = entry['url']
            input_type = entry['type']
            filename = path.basename(input_url)
            output_path = path.join(tmpdirname, filename)
            print(f'Downloading: {input_url} to: {output_path}\n')
            urllib.request.urlretrieve(input_url, output_path)
            temp_url= 'file://' + output_path
            output_url = path.join(destination_url, f'{input_type}.{index:04d}.ndjson')
            print(f'Moving: {temp_url} to: {output_url}\n')    
            dbutils.fs.mv(temp_url, output_url)
            index = index + 1

dbutils.fs.rm(DESTINATION_URL, True)
dowload_export(export_response, DESTINATION_URL)
