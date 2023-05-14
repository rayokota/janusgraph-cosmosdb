#!/bin/bash

# If emulator was started with /AllowNetworkAccess, replace the below with the actual IP address of it:
EMULATOR_HOST=localhost
EMULATOR_PORT=8081
EMULATOR_CERT_PATH=cosmos_emulator.cert
openssl s_client -connect ${EMULATOR_HOST}:${EMULATOR_PORT} </dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > $EMULATOR_CERT_PATH
# delete the cert if already exists
sudo "$JAVA_HOME"/bin/keytool.exe -cacerts -delete -alias cosmos_emulator -storepass changeit -noprompt
# import the cert
sudo "$JAVA_HOME"/bin/keytool.exe -cacerts -importcert -alias cosmos_emulator -file $EMULATOR_CERT_PATH -storepass changeit -noprompt
