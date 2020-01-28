# dbworkbench
Script to fetch, insert data to provided tables.

***Prerequisites***
1. Edit database credentials in core_database.ini file
2. Update primary key id_sequence to latest if db insert is being used.
3. Make app.py executable with chmod +x app.py
4. The project uses python 3.7. You can install the specified version using the following command:
```bash
sudo apt-get install python3.7
```
5. Install pip and venv:
```bash
sudo apt-get install python3-pip
sudo apt-get install python3-venv
```
6. Create and activate new virtual environment:
```bash
cd ~
python3 -m venv myenv
source ~/myenv/bin/activate
```
7. Install requirements:
```bash
pip install -r dbworkbench/requirements.txt
```

****Run the script:****

`bash app.py `

****Arguments:****

| Argument    | Parameter  | Description  |
| ---------------- | ------------ | ------------ |
|`-h or  --help`   | NA   |  show help message and exit |
|`-i or  --insert`  | `table msb msb_id countryId`  | ** table:** table name to insert</br>**msb:** msb name </br> **msb_id: **MSB's company id</br> **countryId:** Destination Country Id. </br>All the data for specified table, msb and country will be added. |
|`-s or --select ` | `table [columns ...]` |   ** table:** table to be fetched </br> **[columns ...]**: list of columns to fetch. Eg: `name id `</br>Get data of specified tables and columns |
|`-l or --limit `    | `limit`  | **Limit:** total number of data to fetch. </br>It can be used along with `--select` argument to limit the no. of data to be fetched. |
|`-e or --enable` | `type countryId companyId ` | Enable payers/bank of specified country id for mto |
