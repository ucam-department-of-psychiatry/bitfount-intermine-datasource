# bitfount-intermine-datasource
Bitfount Datasource for access to Intermine instance

* Clone this repository
* Set up Intermine e.g. https://github.com/intermine/docker-intermine-gradle
* Create a virtual environment. Currently Python 3.9 works with Intermine and Bitfount 2.0. Python 3.8 (and probably 3.9) work(s) with Intermine and Bitfount 1.0.
```
pip install bitfount intermine
mkdir -p ~/.bitfount/_plugins/datasources
ln -s bitfount-intermine-datasource/intermine_source.py ~/.bitfount/plugins/datasources/intermine_source.py
```

* Example code is in ```pod_example.py```
