import boto3
import requests
import csv


def retrieve_country_codes():
    url = "https://static.quandl.com/ECONOMIST_Descriptions/economist_country_codes.csv"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as err:
        print("Unable to retrieve country codes list.")
        raise SystemExit(err)
    decoded_response = response.content.decode('utf-8')
    cr = csv.reader(decoded_response.splitlines(), delimiter='|')
    country_list = list(cr)

    return country_list


def duplicate_rows_for_csv_format(content):
    country_data = []
    row = content
    column_names = content['column_names']
    for value in content["data"]:
        for i in range(0, len(column_names)):
            row[column_names[i]] = value[i]
        row.pop('column_names', None)
        row.pop('data', None)
        country_data.append(row)

    return country_data


def generate_csv_data(start_date, end_date):
    country_codes = retrieve_country_codes()[1:]
    data = []
    for country in country_codes:
        print(country)
        url = 'https://data.nasdaq.com/api/v3/datasets/ECONOMIST/BIGMAC_' + country[1] + \
              '?start_date=' + start_date if start_date else '' + \
              '?end_date=' + end_date if end_date else '' + \
              '&end_date={end_date}&api_key=TqmEsvS3T8xxHcZ_tqRh'
        try:
            response = requests.get(url)
            print(response)
        except requests.exceptions.RequestException as err:
            print('Unable to retrieve dataset.')
            raise SystemExit(err)

        content = response.json()['dataset']
        content['country'] = country[0]
        data = data + duplicate_rows_for_csv_format(content)
    print(data)
    keys = data[0].keys()
    with open('BigMacIndex.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


def upload_to_s3():
    ACCESS_KEY =  ''
    SECRET_ACCESS_KEY = '/FC9FC'
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )
    client.upload_file('BigMacIndex.csv',
                                  'jborowskjtrailbucket2',
                                  'BigMacIndex.csv')


if __name__ == '__main__':
    start_date = '2021-07-01'
    end_date = '2022-07-31'
    generate_csv_data(start_date, end_date)
    upload_to_s3()

