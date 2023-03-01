# Valinor-o offline timestamp processing scripts for Valinor framework

This is the last step in processing the collected timestamps by Valinor-n or Valinor-h frameworks.
The provided Jupyter notebooks read from redis and process the burstiness of timestamp records.

More information can be found at [in our NSDI paper](https://www.usenix.org/conference/nsdi23/presentation/sharafzadeh).

Also checkout [Valinor super repository](https://github.com/hopnets/valinor-artifacts).

### Requirements for Valinor offline analysis scripts

- Redis server running on localhost
- Python3, pip3, and pip3 dependencies listed below:
    - matplotlib
    - redis
    - scipy
    - numpy
    - jupyter-lab
    - statsmodels
    - hurst



## Installation and Running

- Install python with all the dependencies listed above.
- Run jupyter-lab in the background.
- Copy the redis-dump file containing the timestamps to the default Redis location and start redis server. Refer to `load_redis.sh` script for an example of how to load redis-server.
- We have provided an example template file for jupyter-notebook that runs the majority of analysis on timestamp records. Simply run the blocks on the notebooks to load the data from redis and process them.
- Some of the processing scripts require large amounts of memory. We recommend >16GB RAMs on the system.

## Author

Erfan Sharafzadeh

2020-2023