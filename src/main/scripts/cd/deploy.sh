#!/usr/bin/env sh

DIRNAME=$(dirname $0)
DEPLOY_EXEC_FILES=$(find $DIRNAME -name 'deploy-*.sh')

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

mvn -P release deploy -DskipTests=true -B -V -s travis-settings.xml
pip install --user -r requirements.txt
$(dirname $0)/external_build.sh

# extends deploy.sh
for script_file in $DEPLOY_EXEC_FILES; do
    source $script_file
done

