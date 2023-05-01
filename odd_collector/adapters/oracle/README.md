###Usage of OracleDB `thin`/`thick` modes.
By default, python-oracledb runs in a `thin` mode which connects directly to Oracle Database. This mode does not need 
Oracle Client libraries. However, some [additional functionality](https://python-oracledb.readthedocs.io/en/latest/user_guide/appendix_a.html#featuresummary) 
is available when python-oracledb uses them. 
See [Enabling python-oracledb Thick mode](https://python-oracledb.readthedocs.io/en/latest/user_guide/initialization.html#enablingthick).

In **ODD Collector** you can enable thick mode simply by setting `thick_mode=true` (false by default) in your collector_config.yaml file.

***NOTE:*** 
Dockerfile takes care of the installation of the Oracle Instant Client. However, if you want to run the application locally outside of Docker,
you will need to manually install the Oracle Client libraries. You can refer to the [Oracle documentation](https://oracle.github.io/odpi/doc/installation.html) 
for instructions on how to install the Oracle Client libraries based on your operating system. Additionally, 
make sure to configure the PATH environment variable properly to ensure the Oracle Client libraries are accessible to your local environment.
