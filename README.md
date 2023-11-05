# Optimal Reporting Engine (ORE) v1.0 #

## The Approach: ##
Good architecture is modular, separating concerns across its components. This helps ensure each component does one job well, which in turn helps the components work together, delivering a valuable experience to users. Modular architecture is also easier to maintain.
A useful data warehouse does not attempt to take data directly from source systems to end-users in one hit, instead it uses modular components we call data layers. While these modular components have many specific names (e.g. “staging”, “foundation”, “presentation”) what ties them together is their layered nature: each one does a single job well, before passing data on to another layer.
Many of the ideological battles of the past (e.g. Inmon vs Kimball) were founded on an assumption that one methodology must rule them all. As soon as we think about a data warehouse in terms of layers, we are free to choose the optimal methodology for the job each layer is doing.

## What Optimal Reporting Engine (ODE) does: ##
ORE is a multi-tenant configuration engine which helps to build data store using Data Vault methodology

## Requirements: ##
To install ORE, you need to have:

* PostgreSQL server running 
* any IDE for working with PostgreSQL database (DataGrip,EMS...) 
* administrative rights to be able to create objects

## Additional Code ##
https://bitbucket.org/optimalbi/ore_data_store_v1_p1100

## Branches: ##
Currently, ORE has 3 Branches available:

* master
* dev
* dev-clean

## Pre-requisites ##

* Download a copy of the zip and extract to a temporary folder
 
## Installation ##

* Open PostgreSQL IDE and load *ore_release_script_v1_0.sql* from the extracted zip file. You will find it in the *release_script* folder.
* Default schema is *ore_config*
* Click Execute from the toolbar. This should run successfully with a result of 'Completed...' on the Message panel 

## Current functionality: ##
ORE can be viewed as set of blocks each of whose is designed to perform particular function

* config engine 
* config setup
* data vault setup engine
* data vault orchestration engine
* services engine (e.g. logger, scheduler)

Current version 1.0 focus is on implementing first 4 modules of ORE.

From Data vault perspective ORE v1.0 has HUB and SATELLITE functionality implemented.

ORE exists in script creation world and is not involved into script execution. Later versions might include execution engine (queuing tasks, etc.).