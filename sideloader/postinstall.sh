pip="${VENV}/bin/pip"

cd "${INSTALLDIR}/${REPO}/"

$pip install -e "${INSTALLDIR}/${REPO}/"

supervisorctl restart consular
supervisorctl restart consul-template
