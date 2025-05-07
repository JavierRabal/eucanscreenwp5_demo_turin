

#' @title Population data retrieval
#' @description
#' Retrieve Population data by year (1 January), age group (1-year age groups),
#' sex, and NUTS1/NUTS2 region from Eurostat.
#' @param region `[character]` (mandatory, no default)
#'
#' a character vector of region codes (NUTS1/NUTS2 region, e.g. 'AT' = Austria).
#' @param years `[numeric, character]` (mandatory, no default)
#'
#' a vector of dates/years to filter the data.
#' @param age `[integer]` (mandatory, no default)
#'
#' 1-year age groups.
#' @param sex `[character]` (mandatory, no default)
#'
#' a character vector of sex, coded as 'M' = male, 'F' = female, 'T' = total.
#' @seealso [restatapi::get_eurostat_data()] which this function wraps.
#' @export

# get_population_data <- function(
#     region,
#     years,
#     age,
#     sex
# ) {
#   restatapi::get_eurostat_data(
#     id = "demo_r_d2jan",
#     date_filter = years,
#     filters = list(
#       geo = region,
#       age = paste0("Y", age),
#       sex = sex
#     )
#   )
# }
