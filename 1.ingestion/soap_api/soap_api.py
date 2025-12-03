import requests
import psycopg2
import xml.etree.ElementTree as ET

db_conn_str = "host=localhost port=5432 dbname=testdb user=aaron"
url = "https://soap-service-free.mock.beeceptor.com/CountryInfoService.wso"

headers = {
		"Content-Type": "text/xml; charset=utf-8",
		"SOAPAction": "https://soap-service-free.mock.beeceptor.com/CountryInfoService.wso/ListOfCountryNamesByName"
}

body = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
	<soap:Body>
		<ListOfCountryNamesByName xmlns="https://soap-service-free.mock.beeceptor.com/CountryInfoService" />
	</soap:Body>
</soap:Envelope>
"""

def ingest_soap_api_data():
	try:
		with psycopg2.connect(db_conn_str) as conn:
			with conn.cursor() as cur:
				response = requests.post(url, headers=headers, data=body)
				response.raise_for_status()
				soap_xml = response.text
				root = ET.fromstring(soap_xml)
				ns = {
					"soap": "http://schemas.xmlsoap.org/soap/envelope/",
					"m": "http://www.oorsprong.org/websamples.countryinfo",
				}
				for country in root.findall(
					".//m:ListOfCountryNamesByNameResult/m:tCountryCodeAndName", ns
				):
					source_unique_id = country.findtext("m:sISOCode", default="", namespaces=ns).strip()
					raw_record = ET.tostring(country, encoding='unicode')
					cur.execute("truncate table staging_soapapi_records;")
					conn.commit()
					cur.execute("""
					insert into staging_soapapi_records (source_unique_id, raw_record) values (%s, %s);
					""", (source_unique_id, raw_record)
					)
					conn.commit()
					cur.execute("""
					insert into soapapi_records (source_unique_id, raw_record, raw_record_hash, created_on)
					select source_unique_id, raw_record, raw_record_hash, now()
					from staging_soapapi_records
					on conflict (source_unique_id) do update
					set raw_record  = excluded.raw_record,
						raw_record_hash        = excluded.raw_record_hash,
						archived_on = now()
					where soapapi_records.raw_record_hash is distinct from excluded.raw_record_hash;
					""")
					conn.commit()
				cur.execute("truncate table staging_soapapi_records;")
				conn.commit()
	except Exception as e:
		print(e)
		raise

if __name__ == '__main__':
	ingest_soap_api_data()
    # TODO: 
	# - Batch process
    # - Split into functions
    # - Add in-memory diffing
    # - Sparkify