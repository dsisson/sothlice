# sothlice

This is simple base code to: 
* pull tickets from a Jira account
* clean and enrich the ticket data
* push that data into a local postgres database
* pull data from the db and output as CSV files

Why? This is core work necessary to managing certain kinds of quality data. This is a tool I built for my own use.

**Sothicle** is Old English for truly or really.  


## Installation (notes to myself)
### This repo
Copy it locally and create a new repo. No need to fork it, because this has to be heavily customized to a company.


### postgres server
1. download postgres, from [Postgres.app](https://postgresapp.com/downloads.html)
2. install postgres: [Postgres.app documentation](https://postgresapp.com/documentation/install.html)

### postgres client
1. download and configure a client app from that won't drive you crazy.

### db & table structure
1. create the table `qdata` (or whatever you want to call it), and update the code accordingly.


## Functionality

First, you can connect to a Jira account. This is currently configured to work with a standard Jira demo account and its test data. Expect a lot of effort to interface with a real Jira account, projects, and tickets.

Second, you can query against that account, using JQL. This is all very fiddly.

Third, you will have to massage the data model from Jira (because their data model is weird) and convert it into a form that is usable for data metrics tracking. Expect to work at enriching that data.

Fourth, you can create and maintain a database and tables, and use sothlice to read from  and write to those tables.

Fifth, you can export data from the tables into CSV files so that you can share reports. 

Extra credit: you should be able to connect to other data sources for quality-related info and push that into the db. All it takes is some time and effort. 


