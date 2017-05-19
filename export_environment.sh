conda env create -f environment.yml
rm -rf etk_env
conda create -m -p $(pwd)/etk_env/ --copy --clone etk_env
source activate $(pwd)/etk_env
python -m spacy download en
source deactivate $(pwd)/etk_env
zip -r etk_env.zip etk_env