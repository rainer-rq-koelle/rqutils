#' Setup a connection with PRISME
#'
#' @param .drv 
#'
#' @return
#' @export
#'
#' @examples
setup_con <- function(
      .usr = Sys.getenv("USR")
    , .pwd = Sys.getenv("PWD")
    , .dbn = "porape5.ops.cfmu.eurocontrol.be:1521/pe5"
    ){
  
  # prepare environment and formats
  Sys.setenv(
    TZ = "UTC",
    ORA_SDTZ = "UTC",
    NLS_LANG = ".AL32UTF8")
  
  drv    <- ROracle::Oracle()
  schema <-  "PRU_DEV"
  
  con <- DBI::dbConnect(drv = drv,
                      username = .usr,
                      password = .pwd,
                      dbname   = .dbn)
}



construct_sql_apdf_all <- function(.apt, .start, .end){
  query <- paste0(
    "SELECT * FROM SWH_FCT.FAC_APDS_FLIGHT_IR691 WHERE SRC_airport = '", .apt, "'
    and SRC_DATE_FROM >= '", .start,"' 
    and SRC_DATE_FROM < '", .end,"'"
  )
  query <- gsub(pattern = "\n", replacement = "", query)      # trim linebreaks
  query <- gsub(pattern = ("\\s+"), replacement = " ", query) # trim whitespaces
  return(query)
}


#' Clean SQL Query (e.g. remove whitespace, linebreaks) for use in R and DBI
#'
#' @param query 
#'
#' @returns
#' @export
#'
#' @examples
clean_query <- function(query) {
  # remove spaces that occur at the beginning or end of each line
  query <- gsub(pattern = "^\\s+|\\s+$", replacement = "", query)
  # replace multiple spaces inside the query with a single space
  query <- gsub(pattern = "\\s+", replacement = " ", query)
  # optionally, remove line breaks in non-essential places
  query <- gsub(pattern = "\n", replacement = " ", query)
  
  return(query)
}


#' Extract from database
#'
#' @param .con 
#' @param .query 
#'
#' @return
#' @export
#'
#' @examples
get_query <- function(.con, .query){
  res  <- DBI::dbSendQuery(.con, .query)
  data <- DBI::dbFetch(res) |> 
    tibble::tibble()
  # free resources
  DBI::dbClearResult(res)
  # returnd queried dataset
  data
}



# with issue in previous downloads by Thierry, make sure timeformats are what we expect
check_time_columns <- function(.apt, .data){
  my_check <- .data |> 
    summarise(
      across(.cols = c( "MVT_TIME_UTC","BLOCK_TIME_UTC","SCHED_TIME_UTC"
                        ,"C40_CROSS_TIME","C100_CROSS_TIME","PREV_BLOCK_TIME_UTC"),
             .fns = lubridate::is.POSIXct)
    )
  message(paste0(.apt, " - time format check"))
  print(my_check)
} 


# DOWNLOAD APDF
download_apdf <- function(.apt, .start, .end, ...){
  message(paste0("Downloading from APDF: ", .apt, " for ", .start, " to ", .end))
  query <- construct_sql_apdf_all(.apt, .start, .end)
  con   <- setup_con(...)
  res   <- DBI::dbSendQuery(con, query)
  data  <- DBI::dbFetch(res) |> tibble::tibble()
  
  # check time formats for benchmarking
  # check_time_columns(.apt, data)
  return(data)
}


################### WRITE OUT AND SAVE EXTRACTED DATA ########################

# prepare writing out of the retrieved data
# out filename
construct_fn <- function(.apt, .year){
  if(nchar(.year) > 4){.year <- stringr::str_sub(.year, start = nchar(.year)- 3, end = nchar(.year))} 
  fn <- paste0(.apt, "_APDF_", .year)
}

# write out csv and parquet
write_out <- function(.data, .apt, .start){
  out_fn <- construct_fn(.apt, .start)
  print(out_fn)
  .data |> write_csv(file = paste0("./apdf/", out_fn, ".csv.gz"))
  
  # codec_is_available("gzip")
  .data |> write_parquet(
    sink = paste0("./apdf/", construct_fn(.apt, .start), ".gz.parquet")
    , compression = "gzip"
    , compression_level = 5
  )
}

# write out parquet
write_out_parquet <- function(.data, .apt, .start, .path = "./apdf/"){
  out_fn <- construct_fn(.apt, .start)
  print(out_fn)
  if(! arrow::codec_is_available("gzip")){#--------- fallback csv ----------
    message("writing out as csv, no gzip / parquet compression")
    .data |> write_csv(file = paste0(.path, out_fn, ".csv.gz"))
    
  }else{  #--------- default write out as parquet --------------------------
    message("writing out parquet format")
  # arrow::codec_is_available("gzip")
  .data |> arrow::write_parquet(
    sink = paste0(.path, construct_fn(.apt, .start), ".gz.parquet")
    , compression = "gzip"
    , compression_level = 5
  )
  }
}


#================== WRAP ALL ==============================================
get_and_write_apdf_apt <- function(.apt, .start, .end){
  this_apt    <- .apt
  this_start  <- .start
  this_end    <- .end
  ds          <- download_apdf(this_apt, this_start, this_end)
  # write out file
  write_out_parquet(ds, this_apt, this_start)
}
