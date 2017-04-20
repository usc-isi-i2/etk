# Running ETK On Spark

## Setup
* Follow instructions for setup of [ETK](https://github.com/usc-isi-i2/etk)
* Run `.\make.sh`. This will create `etk_env.zip` that creates the environment needed to run ETK ok Spark
* Execcute `.\run.sh` will the parameters as the name of the workflow and the parameters needed by the workflow.
  Example:
  ```
  ./run.sh etkSpacyWorkflow.py /user/effect/data/karma-out/20170420/blog/part-00000
  ```
