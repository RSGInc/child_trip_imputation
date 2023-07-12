
## Setup
Create a virtual environment, activate it, and install the requirements:
```
python -m venv imputeenv
imputeenv\Scripts\activate.bat
pip install -r requirements.txt
```

I did not use conda/mamba because I figure it is best to use a contained local environment if it is to run in the pipeline.

The `requirements.txt` file is generated using `pipreqs` tool, but you need to ignore the virtual environment folder:
```pipreqs ./ --ignore imputeenv```

## Running
To run imputation, you can execute `run.py` as a python script, but it can also be run from command line as `python -m child_trip_imputation`. The latter may be useful for running the imputation program from the pipeline.

### suggest listing available steps here and brief description of what they do
* create_tours: aggregates trips into tours
...

## Settings
The imputation is controlled by the `settings.yaml` file. This contains all the configurable settings, such as Postgres connection settings, input/output file paths, and imputation configuration. This file also contains a variety of parameters, such as buffer distances and column mappings. The settings file is loaded into the `settings.py` module, which is imported by all other modules. This allows the settings to be accessed from anywhere in the code.

## Postgres credentials
To prevent personal login credentials from being uploaded to the repository, the `settings.yaml` file is configured to read the Postgres credentials from a separate file, `.env` file. This file is not tracked by git, so you will need to create it yourself. The file should be formatted as follows:
```
PG_USER = your_username
PG_PWD = your_password
```


## Structure

The code is organized into a hierarchy of python modules and sub-modules:
```
At the top level:
├─ run.py - basic runtime module to run the imputation program
├─ settings.py - global settings module that gets imported by all other modules
|
├─ utils - submodule contains global functions, which inclues:
|   ├─ io.py - the global "database" object which keeps track of the current state of the data tables as well as perform basic I/O functionality.
|   ├─ trip_counter.py - the global "trip counter" object which keeps track of the current trip and joint trip counts and their trip_id's and joint_trip_id's.
|   ├─ misc.py - miscellaneous "static" functions, or any useful function that takes an input and returns an output without changing the global state.
|   ├─ trips_to_tours.py - static function that takes trip table and returns a trip table with tour IDs.
|
├─ nonproxy - submodule relating to imputing proxy-reported trips
|   ├─ impute.py - main impute module runtime for non-proxy trips
|   ├─ populator.py - module to populate the imputed trip, handles the field-wise imputation logic.
|   ├─ timespace_buffer.py - this module flags joint trips and checks for unreported joint trips.
|
├─ school_trips - submodule relating to imputing school trips
|   ├─ impute.py - main impute module runtime for school trips
|   ├─ base_manager.py - base class for table manager methods
|   ├─ household.py - household record manager
|   ├─ person.py - person record manager, inherits household manager
|   ├─ day.py - day record manager, inherits person manager
|   ├─ trip.py - trip record manager, inherits day manager
|   ├─ tour.py - tour record manager, inherits trip manager [not yet implemented]
```

### main modules

#### `run.py`
Basic runtime module to run the imputation program. This module should inherit the subclasses and then run their corresponding methods listed under `STEPS` in `settings.yaml`. 
 
#### `settings.py`
This is the global settings module that gets imported by all other modules. It should also handle any setting processing, such as fetching nested setting parameters or defining defaults.

### utils
This is a submodule contains global functions. It should include anything that is not specific to a particular imputation module.

#### `io.py`
This creates the global `DBIO` "database" that gets instantiated in this module so when it is imported by other modules, they can access and update the same object. This is useful for keeping track of the current state of the data tables, as well as for performing basic I/O functionality. A change in DBIO in any module will be reflected in all other modules.

#### `trip_counter.py` 
This creates a global `TRIP_COUNTER` object. Similar to the `DBIO` object, it is a global "trip counter" which keeps track of the current trip and joint trip counts and their trip_id's and joint_trip_id's.

#### `misc.py`
This contains miscellaneous "static" functions, or any useful function that takes an input and returns an output without changing the global state.

#### `trips_to_tours.py`
This takes trip table and returns determines tour ID based on each "home" purpose. I.e., when the purpose is home, a new tour ID is iterated. 

    NOTE: Currently, this is a static function, but it could be converted to a class to handle more complex tour ID generation in the future. In that case, it should be migrated into its own module folder and inherit the Imputation class.

### nonproxy
This is a submodule relating to imputing proxy-reported trips.

#### `impute.py`
This is the sub-runtime for imputing proxy reported trips. It currently can function as a standalone method independent of the `run.py` runtime.

#### `populator.py`
This class handles the trip field population logic. Each of the methods in this class correspond to the "action" column in the `configs/column_actions_joint_trips.csv` configuration file. 

#### `timespace_buffer.py`
This class handles the time-space buffer logic. It flags joint trips and checks for unreported joint trips. It checks for unreported joint trips by comparing the trip origins, destinations, and times to all other trips in the household on that day. If the trips departed/arrived from the same relative location within the same time window, then it is flagged as a joint trip and the corresponding household members are updated as joint trip participants.

Once this joint trip flagging and unreported joint trip checking is complete, it then checks if there is no record of the other member(s) joint trips, if it does not exist, then the missing joint trip is created. This is done by creating a new trip record and populating it with the methods specified in the configuration file.

### school_trips
This is a submodule relating to imputing school trips.

#### `impute.py`
This is the sub-runtime for imputing school trips. This loops through each household, person, and day; checking if it is missing a school trip. If missing it is imputed using the field imputation methods defined in `trip.py`.

#### `base_manager.py`
This is the base class for table manager methods. It contains holds the single record data as a pandas series and has basic methods for fetching related tables (e.g., all trips for that person or all persons in that household). This method is then inherited by each of the record manager classes.

The base manager also contains the populate() method which is used to populate fields for any imputed record. It will cycle through the named methods each class manager to apply the action to the host record. In this case it is only relevant to trips, but could theoretically be used for other records as well.

#### `household.py`
The household record manager. It inherits the base manager class and is the top level class managers. It has access to the household data e.g., `Household.data` would return the household data.

#### `person.py`
The person record manager. It inherits the household manager class and has access the inherited household data e.g., `Person.data` returns the person record and `Person.Household.data` would return the household record for that person.

#### `day.py`
The day record manager. It inherits the person manager class and has access the inherited person and household data e.g., `Day.data` returns the day record and `Day.Person.data` would return the person record for that day and `Day.Person.Household.data` would return the household record for that person... you get the idea

#### `trip.py`
The trip record manager. It inherits the day manager class and has access the inherited day, person, and household data. More importantly this manager contains the named methods that correspond to the `configs/column_actions_school_trips.csv` configuration file. These methods are used to populate the fields for the imputed trip record.

#### `tour.py`
The tour record manager. It inherits the trip manager class and has access the inherited trip, day, person, and household data. This method is not yet implemented for anything, but could be used to populate tour-level records.
