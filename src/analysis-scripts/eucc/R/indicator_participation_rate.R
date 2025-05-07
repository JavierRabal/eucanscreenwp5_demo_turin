

#' @title Indicator: Participation rate
#'
#' @description
#' The proportion of invited individuals who have undergone a screening
#' test within a given time frame following an active invitation.
#' @details
#' ## Calculation
#'
#' ## Dimensions/ Disaggregation
#' \describe{
#' }
#'
#' ## Further Specifications
#' \describe{
#' }
#'
#' ## Relevant cancer sites
#' \describe{
#' }
#'
#' ## Reference standard
#' N/A
#'
#' @param dt
#' screening data in the CDM format defined in the EUCanScreen project
#' (`data.table`)
#' @param stratum.col.nms
#' names of columns used for stratification as a character vector
#' (default `screening_year`)
#' @param subset
#' a logical expression indicating the rows to keep
#'
#' @returns A `data.table` of ...
#' @examples
#' # example code
#'
#' @export

indicator_participation_rate <- function(
    dt,
    stratum.col.nms = NULL,
    subset = NULL
) {

  if (!missing(subset)) {
    dt <- eval(substitute(dt[i = subset]), envir = dt)
  }

  #' @importFrom data.table .SD .N
  expr <- quote(
    dt[
      j = list(
        "Invited" = sum(.SD[["primary_test_attended"]] %in% c(0, 1), na.rm = TRUE),
        "Attended" = sum(.SD[["primary_test_attended"]] == 1, na.rm = TRUE)
      ), keyby = stratum.col.nms
    ])

  expr[["keyby"]] <- stratum.col.nms

  result <- eval(expr)

  #' @importFrom data.table :=
  result[
    j = "Attended_Perc" := .SD[["Attended"]]/sum(.SD[["Invited"]])*100,
    keyby = stratum.col.nms
  ]
  return(result[])
}
