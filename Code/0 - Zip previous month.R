# Get everything in "Data/"
files_to_zip <- dir("Data", full.names = TRUE)

# Get the reporting month as a string
file_prefix <- "Dec-2023"

# Keep the older A&E data
files_to_zip <- files_to_zip[! files_to_zip %in% c("Data/3-A&E-1415.parquet",
                                                   "Data/3-A&E-1516.parquet",
                                                   "Data/3-A&E-1617.parquet",
                                                   "Data/3-A&E-1718.parquet",
                                                   "Data/3-A&E-1819.parquet",
                                                   "Data/3-A&E-1920.parquet",
                                                   "Data/3-A&E-2021.parquet",
                                                   "Data/3-A&E-2122.parquet",
                                                   "Data/3-A&E-2223.parquet",
                                                   "Data/3-Complete-Years.parquet")]

# Subset breakdowns and main indicator files
breakdowns <- files_to_zip[stringr::str_detect(files_to_zip, 
                                               stringr::regex("breakdown", ignore_case = TRUE))]
indicators <- setdiff(files_to_zip, breakdowns)

# Zip the files in "Archive/"
zip(zipfile = glue("Archive/{file_prefix}-Breakdowns"), files = breakdowns)
zip(zipfile = glue("Archive/{file_prefix}-Indicators"), files = indicators)

# Tidy up environment
rm(files_to_zip,
   file_prefix,
   breakdowns,
   indicators)

# WARNING: this deletes all the unzipped files so only use it when you're sure
# unlink(files_to_zip)
