
library(duckdb)
library(data.table)
library(popEpi)

path_database <- '/home/mambauser/projects/inputs/data.duckdb'  

## scr_episode table
con = dbConnect(duckdb::duckdb(), dbdir=path_database, read_only=FALSE)
logger::log_info("Connect to database")
df <- dbGetQuery(con, paste0(
  "SELECT * FROM screening_episode"))
data.table::setDT(df)

## variable scr_year
df[j = "scr_year" := floor(
  popEpi::get.yrs(df[["primary_attended_date"]])
  )]

#######################
## calculate indicators
#######################

## make function
scr_stat_aggregate <- function(
    dt, 
    value.col.nms,
    stratum.col.nms = NULL, 
    aggre.fun = sum,
    dt.subset = NULL
) {
  value_col_nms <- value.col.nms
  stratum_col_nms <- stratum.col.nms
  #' @importFrom data.table .SD
  expr <- quote(lapply(.SD, aggre.fun, na.rm = T))
  
  x_expr <- quote(dt[])
  x_expr[["j"]] <- expr
  x_expr[[".SDcols"]] <- value_col_nms
  if (!is.null(stratum_col_nms)) {
    x_expr[["keyby"]] <- stratum_col_nms
  }
  if (!is.null(dt.subset)) {
    x_expr[["i"]] <- quote(eval(parse(text = dt.subset)))
  }
  result_dt <- eval(x_expr)
  
  return(result_dt[])
} 


# test on data
stat_aggre <- scr_stat_aggregate(
  dt = df,
  value.col.nms = c("primary_invited", "primary_attended"),
  stratum.col.nms = "scr_year"
  )

data.table(fwrite(stat_aggre, file = '/home/mambauser/projects/outputs/stat_aggre.csv'))

