

#' @title Indicator: Test result
#'
#' @description
#' The results of the screening test.
#' Monitoring the different results of the screening test has many benefits.
#' The number of positive screening tests determines the cancer diagnostic
#' assessment workload and cancer detection rate and is important for program
#' planning and quality assurance. Any substantial changes to these results
#' over time will require investigation.
#'
#' @details
#' ## Calculation
#' N = Number of tests with either positive, negative, inadequate, or
#' borderline results \cr
#' D = Total number of screening tests within the program
#'
#' ## Dimensions/ Disaggregation
#' \describe{
#'  \item{*Minimum_Requirements:*}{Sex, Age Group, Screening Test Type}
#'  \item{*Desirable:*}{Education, Income, Migration Status, Country of Birth,
#' Country of Citizenship, Country of Parents Birth, Employment Status,
#' Disability Status, Screening Invitation Round, Previous Screening Attendance}
#'  \item{*Future:*}{Risk Status, LGBTIQA+ status, Geographic Location, Deprivation Area,
#' Vaccination status}
#' }
#'
#' ## Further Specifications
#' \describe{
#'  \item{*Breast:*}{Positive, negative, inadequate, unknown --
#'  Management: Positive: referral to further assessment. Negative: not
#'  referred to further assessment. Inadequate: test result inadequate, retest
#'  needed. Unknown: test result not know.}
#'  \item{*Cervical:*}{Positive, borderline, negative, inadequate, unknown --
#'  Management: Positive: referral to triage cytology or colposcopy.
#'  Borderline: Recommendation to follow up testing Negative: not referred to
#'  colposcopy. Inadequate: test result inadequate, retest needed. Unknown:
#'  test result not known.}
#'  \item{*Colorectal:*}{Positive, negative, inadequate, unknown --
#'  Management: Positive: referral to colonoscopy (diagnostic confirmation).
#'  Negative: not referred to colonoscopy (test result known). Inadequate:
#'  test result inadequate, retest needed. Unknown: test result not known.
#'  Note: result depending on the test system.}
#'  \item{*Lung:*}{Positive, negative, indeterminate, inadequate, unknown --
#'  Management: Positive: referral to further assessment. Negative: regular
#'  follow-up LDCT (usually after 12 months). Indeterminate: need for early
#'  rescreen (LDCT already after 3 or 6 months). Inadequate: test result
#'  inadequate, retest needed. Unknown: test result not known.}
#' }
#'
#' ## Relevant cancer sites
#' \describe{
#'  \item{*Current:*}{Breast, Cervical, Colorectal, Prostate, Lung}
#'  \item{*Future:*}{}
#' }
#'
#' ## Reference standard
#' N/A
#'
#' @param dt
#' screening data in the CDM format defined in the EUCanScreen project
#' (`data.table`)
#' @param result.col.nm
#' name of the categorical result column as a character (default
#' `"primary_result_factor"`)
#' @param stratum.col.nms
#' names of columns used for stratification as a character vector
#' (default `NULL`)
#' @param subset
#' a logical expression indicating the rows to keep
#' @param na.rm
#' a logical expression; if `TRUE`, drops rows that have `NA` values in
#' column result.col.nm (default `FALSE`)
#'
#' @returns A `data.table` of counts and percentages of each result category,
#' further stratified by the columns defined in `stratum.col.nms`.
#' @examples
#' # example code
#'
#' @export

indicator_test_result <- function(
    dt,
    result.col.nm = "primary_test_result_factor",
    stratum.col.nms = NULL,
    subset = NULL,
    na.rm = TRUE
) {

  if (!missing(subset)) {
    dt <- eval(substitute(dt[i = subset]), envir = dt)
  }
  #' @importFrom data.table .SD .N
  expr <- quote(dt[j = .N])

  if (is.null(stratum.col.nms)) {
    expr[["keyby"]] <- result.col.nm
  } else {
    expr[["keyby"]] <- c(stratum.col.nms, result.col.nm)
  }
  if(na.rm == TRUE) {
    expr[["i"]] <- quote(!is.na(dt[[result.col.nm]]))
  }
  result <- eval(expr)

  #' @importFrom data.table :=
  result[
    j = "Percentage" := .SD[["N"]]/sum(.SD[["N"]])*100,
    keyby = stratum.col.nms
  ]

  return(result[])
}



