
tl_settings_cx <- data.table::fread("data-raw/tl_settings_cx.csv")
tl_settings_cx[, col_nm_set := strsplit(tl_settings_cx[["col_nm_set"]], ",")]

# fcrdev::pkg_sysdata_write_object(tl_settings_cx)
