

create_tl <- function(dataset, settings) {
  tl <- lapply(seq_len(nrow(settings)), function(i) {
    al <- eval(parse(text = settings[["arg_list"]][[i]]))
    al[["stratum.col.nms"]] <- settings[["col_nm_set"]][[i]]
    al[["dt"]] <- dataset
    do.call(settings[["fun_nm"]][[i]], args = al)
  })
  names(tl) <- settings[["tab_nm"]]
  return(tl)
}
