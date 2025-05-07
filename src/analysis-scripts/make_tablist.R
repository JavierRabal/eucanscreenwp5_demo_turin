
library(duckdb)
library(data.table)
library(jsonlite)

path_database <- '/home/mambauser/projects/inputs/data.duckdb'

devtools::load_all(path = "/home/mambauser/projects/src/analysis-scripts/eucc")



## read data
entity_name <- c(
  "person", "screening_round", "primary_phase", 
  "assessment_phase", "treatment_phase"
)

con = dbConnect(duckdb::duckdb(), dbdir=path_database, read_only=FALSE)
logger::log_info("Connect to database")

df <- lapply(1:length(entity_name), function(i) {
  df <- dbGetQuery(con, paste0(
    "SELECT * FROM ", entity_name[i]))
  data.table::setDT(df)
  df
})
names(df) <- entity_name
df


## merge entities
dt <- cbind( ## just paste person & round together for synthetic data to keep all rows (ids dont match)
  df[["person"]], df[["screening_round"]][, -c("person_id")]
)
dt <- merge(
  dt, df[["primary_phase"]],
  by = "round_id", all.x = TRUE
)
dt <- merge(
  dt, df[["assessment_phase"]],
  by = "round_id", all.x = TRUE
)
dt <- merge(
  dt, df[["treatment_phase"]],
  by = "round_id", all.x = TRUE
)
dt



## make tablist
tl <- eucc:::create_tl(
  dataset = dt,
  settings = eucc:::tl_settings_cx
)


## merge labels
migration_status <- data.table::data.table(
  migration_status = as.character(c(0, 1, 99)), 
  migration_status_label = c("non-migrant", "migrant", "unknown")
)
primary_test_result_factor <- data.table::data.table(
  primary_test_result_factor = c(0, 1, 2, 9, 99), 
  primary_test_result_factor_label = c("negative", "positive", "borderline", "inadequate", "unknown")
)
primary_test_method <- data.table::data.table(
  primary_test_method = c(1, 2),
  primary_test_method_label = c("Pap smear", "HPV test")
)


for(var in c("migration_status", "primary_test_result_factor", "primary_test_method")) {
  for(i in 1:length(tl)) {
    if(var %in% names(tl[[i]])) {
      tl[[i]] <- merge(tl[[i]], eval(parse(text = var)), by = var, sort = FALSE)
      data.table::set(tl[[i]], j = var, value = NULL)
      data.table::setcolorder(tl[[i]], paste0(var, "_label"))
    }
    if("screening_year" %in% names(tl[[i]])) {
      data.table::setcolorder(tl[[i]], "screening_year")
    }
  }
}
