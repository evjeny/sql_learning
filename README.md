# sql_learning
Lessons of SQL

***

[Basic SQL course](https://www.codecademy.com/learn/learn-sql)

Plan:
* [x] [Manipulations](create_tables.py) (`create table`, `insert into`, `select`, `alter table`, `update`, `delete from`)
* [x] [Queries](db_overview.py) (`select`, `as`, `distinct`, `where`, `like`, `and`, `or`, `order by`, `limit`, `case`)
* [x] [Aggregations](db_groups.py) (`count`, `sum`, `min/max`, `avg`, `round`, `group by`, `having`)
* [x] [Tables ops](db_sales.py) (`join`, `left join`, `cross join`, `union`, `with`)

### Installation

1. Install Anaconda package manager
2. Run in console: `conda env create -f environment.yml` to create conda environment
3. Activate env in console: `conda activate sql_learning`

### Run examples

1. Generate database: `python create_tables.py`
2. Run examples:
```bash
python db_overview.py
python db_groups.py
python db_sales.py
```
