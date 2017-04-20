#WORKFLOWSBASE=/mnt/github/dig-workflows

CONDA_PY_DEST=etk_env/lib/python2.7/site-packages
ETK_BASE=../..

rm -rf etk_env
rm etk_env.zip

#Clone the Conda environment
conda create -m -p $(pwd)/etk_env/ --copy --clone etk_env

#Copy all etk files inro the conda site packages
#mkdir $CONDA_PY_DEST/digWorkflow
#cp $WORKFLOWSBASE/pySpark-workflows/digWorkflow/* $CONDA_PY_DEST/digWorkflow/ 

mkdir $CONDA_PY_DEST/etk
cp -rf $ETK_BASE/etk/* $CONDA_PY_DEST/etk/

# Now zip into archive that can be added to spark
zip -r etk_env.zip etk_env
mkdir etk_env/etk_env
mv etk_env/* etk_env/etk_env/
