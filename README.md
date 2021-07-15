# TN-dev-prod-database-merge-script
## Introduction
This is a python script that generates the list of SQL queries needed to merges two MySQL databases having the same schema but different data. 
<br>
This script is schema specific to the TN database.
<br>
It takes into consideration the AUTO_INCREMENT PRIMARY KEYS and the corresponding FOREIGN KEYS.
<br><br>
<strong>This script is still in construction! </strong>
## Requirements
- The database SQL dump files
- A text file for saving the queries needed for the merge

## Note
After running this script the source database must be cleaned up before running it again.
