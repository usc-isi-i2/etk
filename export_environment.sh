conda env create -f environment.yml
source activate etk_env
python -m spacy download en
source deactivate etk_env
rm -rf etk_env
conda create -m -p $(pwd)/etk_env/ --copy --clone etk_env
zip -r etk_env.zip etk_env