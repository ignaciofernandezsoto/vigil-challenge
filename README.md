# Vigil Challenge

## Instructions
- In a separate pdf file

## Assumptions
- The S3 input path is a directory path in an S3 bucket which may contain files different from .csv and .tsv.
- The S3 output path is a directory path in an S3 bucket. Existing files with the same name will be replaced by the new version.
- The S3 input and output paths should use the s3a protocol. For instance: s3a://vigil-files/input/

## Features
- Stream usage. The app can work with incredible big files, as the data is processed in a stream.
- More than 60% code coverage.
- Able to run on Docker

## How it works
1) The request is parsed and validated. If it badly formatted or lacks an argument, an error is returned.
2) If the request is valid, then the input data file is retrieved. If it doesn't exist, or we lack the permissions to retrieve it, an error is returned.
3) If the input data file is retrieved correctly, then the key/values are sanitized. This means that the "stringified" integers are transformed to integers. If the string is null or empty, a zero is returned.
4) Once the data is sanitized, the data reduction is done. For each key for their multiple values, there exists a single value which occurs an odd amount of times. So, for each key, a key-value is returned which satisfies this condition.
5) Finally, the reduced data is written in S3 in .tsv format.

## How to use
### Requirements
- SBT
- Docker

### How to run
- `sbt docker:publishLocal`
- `docker run -e AWS_ACCESS_KEY_ID={aws_access_key_id} -e AWS_SECRET_ACCESS_KEY={aws_secret_access_key} --rm vigil-challenge:0.1.0-SNAPSHOT
  {s3a_input_url} {s3a_output_url}`

### Run example
`docker run -e AWS_ACCESS_KEY_ID=abc -e AWS_SECRET_ACCESS_KEY=xyz --rm vigil-challenge:0.1.0-SNAPSHOT s3a://vigil-files/input/ s3a://vigil-files/output/`

### Return example
```
[...]
23/04/02 16:23:43 INFO Main: Success! 4 files were successfully created
[...]
```

## Technical debt
- Include refined as a library. This would make the app more robust.
- Replace `read` for `readStream` on Spark read method for performance boost.