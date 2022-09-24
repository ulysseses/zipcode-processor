import os
import sys

import requests


URL = 'https://redline-redline-zipcode.p.rapidapi.com/rest/multi-radius.json/30/mile'
REQUEST_BATCH_SIZE = 99
HEADERS = {
  'content-type': 'application/x-www-form-urlencoded',
  'X-RapidAPI-Host': 'redline-redline-zipcode.p.rapidapi.com',
  'X-RapidAPI-Key': 'ca6471f5e2msh9b26de43a9c98a1p107c17jsnbfe6368b5b0e',
}


def batches(lst, size=REQUEST_BATCH_SIZE):
  for i in range(0, len(lst), size):
    yield lst[i : i + size]


def input2output(path):
  tup = os.path.splitext(path)
  return tup[0] + '.out' + tup[1]


def valid_zip_code(s):
  # A valid zip code is of length 5
  # and consists only of digits.
  return len(s) == 5 and s.isnumeric()


def main():
  # Load zip codes file
  input_path = os.path.abspath(sys.argv[1])
  with open(input_path, 'r') as f:
    input_zip_codes = [line.rstrip() for line in f if valid_zip_code(line.rstrip())]

  # Send a batch of zip codes to request,
  # and write to output CSV.
  output_zip_codes = set()
  n_batches = len(list(batches(input_zip_codes)))
  for i, batch in enumerate(batches(input_zip_codes)):
    print(f'Processing batch {i + 1}/{n_batches} of size {len(batch)}')
    # Send request and receive response
    payload = f'zip_codes={"%0A".join(batch)}'
    response = requests.request('POST', URL, data=payload, headers=HEADERS)
    json = response.json()
    # Unmarshal base_zip_code and zip_codes from response.
    try:
      for response in json['responses']:
        output_zip_codes.update(response['zip_codes'])
    except KeyError:
      print(f'response json:\n{json}')
      print(f'batch: {batch}')
      sys.exit(1)

  print(f'Found a total of {len(output_zip_codes)} zip codes')

  # Write outputs to CSV, one line per zip_code in zip_codes
  output_path = input2output(input_path)
  with open(output_path, 'w') as f:
    # CSV header
    f.write('zip_code\n')
    for zip_code in sorted(output_zip_codes):
      f.write(f'{zip_code}\n')

  print('Done!')


if __name__ == '__main__':
  main()
