﻿# Optimal Reporting Engine (ORE) v1.0 #

## The Approach: ##
Good architecture is modular, separating concerns across its components. This helps ensure each component does one job well, which in turn helps the components work together, delivering a valuable experience to users. Modular architecture is also easier to maintain.
A useful data warehouse does not attempt to take data directly from source systems to end-users in one hit, instead it uses modular components we call data layers. While these modular components have many specific names (e.g. “staging”, “foundation”, “presentation”) what ties them together is their layered nature: each one does a single job well, before passing data on to another layer.
Many of the ideological battles of the past (e.g. Inmon vs Kimball) were founded on an assumption that one methodology must rule them all. As soon as we think about a data warehouse in terms of layers, we are free to choose the optimal methodology for the job each layer is doing.

## What Optimal Reporting Engine (ODE) does: ##
ORE is a multi-tenant configuration engine which helps to build data store using Data Vault methodology


## Requirements (All users): ##
To install ORE, you need to have:
* PostgreSQL server running 
* any IDE for working with PostgreSQL database (DataGrip,EMS...) 
* administrative rights to be able to create objects

If you wish to develop ODE further, we recommend:
* Visual Studio 2015 Community Edition installed on a computer which you can access. This can be downloaded from https://www.visualstudio.com/en-us/mt171547.aspx

## Additional Code ##
The related project "optimal-data-engine-mssql-scripts" in the OptimalBI repository contains a set of scripts, which can be downloaded and used to assist in making use of ODE.

## Branches: ##
Currently, ODE has two Branches available:
* master and
* develop

Master contains code which is known to be running in a production environment.

Develop contains the latest, locally tested version of the codebase.

## Download and build instructions: ##
If you wish to develop the code base further, we recommend that you use the Visual Studio solution which is provided.

If you simply wish to build an ODE instance and use it, the following instructions will direct you:

### Pre-requisites ###

* Download a copy of the zip and extract to a temporary folder
 
### Scripted Install ###

* Open SQL Server Management studio and load *ode_to_mssql_Create.sql* from the extracted zip file. You will find it in the *ReleaseScript* folder.
* Within SQL Server Management Studio > Click Query Menu > SQLCMD Mode 
* Within the script optionally change the DatabaseName and DefaultFilePrefix in the code to the preferred Configuration database name; default is *ode_to_mssql*. *ODE_Config* is recommended. 
* Click Execute from the toolbar. This should run successfully with a result of 'Update complete' on the Message panel 
* The Results panel of Management Studio query execution window should show 13 rows, which are the contents of the log4.JournalControl table.

### Manual Install ###

* This method expects that the user has a good understanding of Visual Studio Data Tools.
* Download the Optimal Data Engine code from GitHub, into a directory, e.g. "C:\Git\OptimalDataEngine\"
* Using Visual Studio, open the *ode_to_mssql.sln* solution.
* Publish the Database to your server - preferred name for the database is *ODE_Config*.

## Current functionality: ##
Details of the current functionality can be found here http://www.ode.ninja/category/features/

## Notes ##
* Untested on SQL Server editions prior to 2014
* This product is still in Beta and should not be deployed to a production environment without thorough testing by you to ensure no adverse effects on your environment

## Feedback, suggestions, bugs, contributions: ##
Please submit these to GitHub issue tracking or join us in developing by forking the project and then making a pull request!

## Find out more: ##
Visit http://www.ode.ninja/ - this is where we keep our guides and share our knowledge. To find out more about OptimalBI and the work we do visit http://www.optimalbi.com or check out our blogs at http://optimalbi.com/blog/tag/data-vault/ for all the latest on our Data Vault journey. If you want to get in touch, you can email us at hey@optimalbi.com