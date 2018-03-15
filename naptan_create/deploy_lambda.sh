#!/usr/bin/env bash

# zip package
zip -r lambda.zip . -x ".*"

# Upload zip to AWS (Update code)
aws --region eu-west-1 lambda update-function-code --function-name createNaptanTable --zip-file fileb://"$(pwd)"/lambda.zip

# remove zip file
rm lambda.zip
