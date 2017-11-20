#' getConnectionParameters
#'
#' @details
#' Loads connection parameters from a json file.
#'
#' The JSON file should have the following format:
#'
#' \code{[ \cr
#'    {	\cr
#'      "dbms":"<dbms>",\cr
#'      "server":"<host-ip>",\cr
#'      "dbname":"<PostgreSQL db name>",\cr
#'      "user":"<user>",\cr
#'      "password":"<password>",\cr
#'      "sourceschema":"<database schema>",\cr
#'      "targetschema":"<database schema>"\cr
#'   }\cr
#' ] \cr
#' }
#'
#' \code{dbms} can be one of "postgresql", "sqlserver", "oracle *", "mysql *", or "redshift *" (* not yet implemented)\cr
#' \code{dbname} is only applicable for PostgreSQL databases
#'
#' @param connectionFile   the full path to the json file
#'
#' @return A string JSON object containing details for connecting to the database
#'
#' @export

getConnectionParameters <- function(connectionFile) {
  json <- NA
  if(file.exists(connectionFile)){
    json<-read_file(connectionFile)
  }
}

#' createConnectionParameters
#'
#' @details
#' Create connection parameters json from a set of parameters.
#'
#' @param dbms            one of "postgresql", "sqlserver", "oracle *", "mysql *", or "redshift *" (* not yet implemented)
#' @param host            the ip address/host name of the database instance
#' @param dBname          for PostgreSQL database only: the database name
#' @param dBsourceSchema  the source database schema
#' @param dBtargetSchema  the target database schema
#' @param user            the user name
#' @param password        the user password
#'
#' @return A string JSON object containing details for connecting to the database
#'
#' @export

createConnectionParameters <- function(
  dbms,
  host,
  dBname = NA,
  dBsourceSchema,
  dBtargetSchema,
  user,
  password
) {

  params <- paste(
    '[
    {
    "dbms":"',dbms,'",',
    '"server":"',host,'",',
    '"dbname":"',dBname,'",',
    '"sourceschema":"',dBsourceSchema,'",',
    '"targetschema":"',dBtargetSchema,'",',
    '"user":"',user,'",',
    '"password":"',password,'"
    }
    ]',sep='')

  return(params)
    }

#' readinteger(promptStr)
#'
#' @details
#' Prompts the user to enter an integer
#'
#' @param promptStr  The string to display when asking user for input.
#'
#' @return An integer.
#'

readinteger <- function(promptStr)
{
  n <- readline(prompt=promptStr)
  if(!grepl("^[0-9]+$",n))
  {
    return(readinteger())
  }

  return(as.integer(n))
}


#' insertCohortResultRecords
#'
#' @details
#' Inserts the cohort inclusion results resords in the specified schema using the specified cohort ID.
#' NOTE: INCOMPLETE!! Currently only works when the cohort results tables are in the default schema!
#'
#' @param connectionParameters  a string JSON object containing details for connecting to the database
#' @param cohortID              the cohort_definition_id for which to insert the result records
#' @param jsonResults           a JSON string containing the cohort inclusion result records
#'

insertCohortResultRecords <- function(connectionParameters, cohortID, jsonResults) {
  # extract the parameters
  axParams <- fromJSON(connectionParameters)
  axDbms = axParams[["dbms"]]
  axHost = axParams[["server"]]
  axDBname = axParams[["dbname"]]
  axDBsourceSchema = axParams[["sourceschema"]]
  axDBtargetSchema = axParams[["targetschema"]]
  axUser = axParams[["user"]]
  axPassword = axParams[["password"]]
  axDBport = 0

  switch (axDbms,
          postgresql = {
            # append database name to connection string for PostgreSQL instance
            # axHost <- paste(axHost,'/',axDBname)

            # load the PostgreSQL driver
            drv <- dbDriver("PostgreSQL")

            axDBport = 5432

            # create a connection to the database
            con <- dbConnect(drv, dbname = axDBname, host = axHost, port = axDBport, user = axUser, password = axPassword)
          },
          sqlserver = {
            drv <- dbDriver("SQLServer")

            axDBport = 1433
            # create a connection to the database
            con <- dbConnect(drv, server = axHost, properties=list(user=axUser, password=axPassword))
          },
          stop("Unknown database type.")
  )

#  cohort_def <- data.frame(jsonResults['cohort_definition'])
#  colnames(cohort_def) <- c("id","name","description","expression_type","created_by","created_date","modified_by","modified_date")
#  cohort_def$id[cohort_def$id != cohortID] <- cohortID
#  dbWriteTable(con, "cohort_definition", cohort_def, append=TRUE)
#
#  cohort_def_details <- data.frame(jsonResults['cohort_definition_details'])
#  colnames(cohort_def_details) <- c("id","expression")
#  cohort_def_details$id[cohort_def_details$id != cohortID] <- cohortID
#  dbWriteTable(con, "cohort_definition_details", cohort_def_details, append=TRUE)

  cohort_inc <- data.frame(jsonResults['cohort_inclusion'])
  colnames(cohort_inc) <- c("cohort_definition_id","rule_sequence","name","description")
  cohort_inc$cohort_definition_id[cohort_inc$cohort_definition_id != cohortID] <- cohortID
  dbWriteTable(con, "cohort_inclusion", cohort_inc, append=TRUE)

  cohort_inc_result <- data.frame(jsonResults['cohort_inclusion_result'])
  colnames(cohort_inc_result) <- c("cohort_definition_id","inclusion_rule_mask","person_count")
  cohort_inc_result$cohort_definition_id[cohort_inc_result$cohort_definition_id != cohortID] <- cohortID
  dbWriteTable(con, "cohort_inclusion_result", cohort_inc_result, append=TRUE)

  cohort_inc_stats <- data.frame(jsonResults['cohort_inclusion_stats'])
  colnames(cohort_inc_stats) <- c("cohort_definition_id","rule_sequence","person_count","gain_count","person_total")
  cohort_inc_stats$cohort_definition_id[cohort_inc_stats$cohort_definition_id != cohortID] <- cohortID
  dbWriteTable(con, "cohort_inclusion_stats", cohort_inc_stats, append=TRUE)

  cohort_sum_stats <- data.frame(jsonResults['cohort_summary_stats'])
  colnames(cohort_sum_stats) <- c("cohort_definition_id","base_count","final_count")
  cohort_sum_stats$cohort_definition_id[cohort_sum_stats$cohort_definition_id != cohortID] <- cohortID
  dbWriteTable(con, "cohort_summary_stats", cohort_sum_stats, append=TRUE)

  # disconnect and unload
  dbDisconnect(con)
  dbUnloadDriver(drv)
}


#' getCohortResults
#'
#' @details
#' Returns the cohort result records for the specified cohort in a JSON structure.
#'
#' @param connectionParameters  a string JSON object containing details for connecting to the database
#' @param cohortID              the cohort_definition_id for which to extract the results
#'
#' @return                      JSON string with results from the executed SQL script
#'

getCohortResults <- function(connectionParameters, cohortID) {
  # extract the parameters
  axParams <- fromJSON(connectionParameters)
  axDbms = axParams[["dbms"]]
  axHost = axParams[["server"]]
  axDBname = axParams[["dbname"]]
  axDBsourceSchema = axParams[["sourceschema"]]
  axDBtargetSchema = axParams[["targetschema"]]
  axUser = axParams[["user"]]
  axPassword = axParams[["password"]]
  axDBport = 0

  switch (axDbms,
    postgresql = {
      # append database name to connection string for PostgreSQL instance
      # axHost <- paste(axHost,'/',axDBname)

      # load the PostgreSQL driver
      drv <- dbDriver("PostgreSQL")

      axDBport = 5432

      # create a connection to the database
      con <- dbConnect(drv, dbname = axDBname, host = axHost, port = axDBport, user = axUser, password = axPassword)
    },
    sqlserver = {
      drv <- dbDriver("SQLServer")

      axDBport = 1433
      # create a connection to the database
      con <- dbConnect(drv, server = axHost, properties=list(user=axUser, password=axPassword))
    },
    stop("Unknown database type.")
  )

  cohortID_str<-as.character(cohortID)
  # execute SQL queries to retrieve the resulting records, replacing the parameters
  cohort_def_data<-dbGetQuery(con,
    paste(gsub("@target_database_schema",axDBtargetSchema,
      gsub("@target_cohort_id", cohortID_str,"
        select id,
        name,
        description,
        expression_type,
        created_by,
        created_date,
        modified_by,
        modified_date
        from @target_database_schema.cohort_definition
        where id=@target_cohort_id;")), sep="")
  )

  cohort_def_details_data<-dbGetQuery(con,
    paste(gsub("@target_database_schema",axDBtargetSchema,
      gsub("@target_cohort_id", cohortID_str,"
        select id,
        expression
        from @target_database_schema.cohort_definition_details
        where id=@target_cohort_id;")), sep="")
  )
  cohort_inc_data<-dbGetQuery(con,
    paste(gsub("@target_database_schema",axDBtargetSchema,
      gsub("@target_cohort_id", cohortID_str,"
        select cohort_definition_id,
        rule_sequence,
        name,
        description
        from @target_database_schema.cohort_inclusion
        where cohort_definition_id=@target_cohort_id;")), sep="")
  )
  cohort_inc_result_data<-dbGetQuery(con,
    paste(gsub("@target_database_schema",axDBtargetSchema,
      gsub("@target_cohort_id", cohortID_str,"
        select cohort_definition_id,
        inclusion_rule_mask,
        person_count
        from @target_database_schema.cohort_inclusion_result
        where cohort_definition_id=@target_cohort_id;")), sep="")
  )
  cohort_inc_stats_data<-dbGetQuery(con,
    paste(gsub("@target_database_schema",axDBtargetSchema,
      gsub("@target_cohort_id", cohortID_str,"
        select cohort_definition_id,
        rule_sequence,
        person_count,
        gain_count,
        person_total
        from @target_database_schema.cohort_inclusion_stats
        where cohort_definition_id=@target_cohort_id;")), sep="")
  )
  cohort_sum_stats_data<-dbGetQuery(con,
    paste(gsub("@target_database_schema",axDBtargetSchema,
      gsub("@target_cohort_id", cohortID_str,"
        select cohort_definition_id,
        base_count,
        final_count
        from @target_database_schema.cohort_summary_stats
        where cohort_definition_id=@target_cohort_id;")), sep="")
  )

  # disconnect and unload
  dbDisconnect(con)
  dbUnloadDriver(drv)

  resultsJSON<- paste("{\"cohort_definition\": ", toJSON(cohort_def_data, null='list', na='null'),",",
                      "\"cohort_definition_details\":", toJSON(cohort_def_details_data, null='list', na='null'), ",",
                      "\"cohort_inclusion\": ", toJSON(cohort_inc_data, null='list', na='null'), ",",
                      "\"cohort_inclusion_result\": ", toJSON(cohort_inc_result_data, null='list', na='null'), ",",
                      "\"cohort_inclusion_stats\": ", toJSON(cohort_inc_stats_data, null='list', na='null'), ",",
                      "\"cohort_summary_stats\": ", toJSON(cohort_sum_stats_data, null='list', na='null'), "}",
                      sep="")

  return(resultsJSON)
}

#' exportCohortResults
#'
#' @details
#' Extracts the cohort_inclusion, cohort_inclusion_result, cohort_inclusion_stats,
#' and cohort_summary_stats records for a cohort definition.
#' Saves the resulting cohort result records as a JSON file.
#'
#' @param connectionParameters  a string JSON object containing details for connecting to the database
#' @param outFilePath           file path to where the results should be written
#'
#' @export

exportCohortResults <- function(connectionParameters, outFilePath) {
  # Get the cohort ID from the user
  cohortId <- readinteger("Please enter the cohort ID [integer]:")

  # Execute a set of SQL scripts and get resulting cohort records in a JSON string
  cohortResults <-
    getCohortResults(connectionParameters, cohortID = cohortId)

  if (!is.na(cohortResults)) {
    # Write the resulting cohort records to a JSON file
    write(cohortResults, outFilePath)
  }
}

#' importCohortResults
#'
#' @details
#' Loads the cohort inclusion results JSON file, and uploads/imports
#' the records to the corresponding tables in the specified schema
#' with the specified cohort ID.
#' NOTE: INCOMPLETE!! Currently only works when the cohort results tables are in the default schema!
#'
#' @param connectionParameters  a string JSON object containing details for connecting to the database
#' @param inFilePath            file path to the cohort inclusion results JSON file
#'
#' @export

importCohortResults <- function(connectionParameters, inFilePath) {
  # Get the cohort ID from the user
  cohortId <- readinteger("Please enter the cohort ID [integer]:")

  # Import the JSON file
  cohortResults <- fromJSON(inFilePath)
  insertCohortResultRecords(connectionParameters, cohortID = cohortId, jsonResults = cohortResults)
}
