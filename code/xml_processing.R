library(tidyverse)
library(XML)
library(xml2)
library(progress)
library(arrow)

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024")

  # це дістаємо з архівів XML-файли, розбираємось з папками

if (!require(archive)) {
  install.packages("archive")
  library(archive)
}

# Function to extract with archive package
extract_with_archive <- function(zip_file, extract_dir) {
  tryCatch({
    # First, check what's in the archive
    archive_info <- archive(zip_file)
    print(head(archive_info))
    
    # Extract with archive package (better at handling filename issues)
    archive_extract(zip_file, dir = extract_dir)
    
    return(TRUE)
  }, error = function(e) {
    cat("Archive extraction failed:", e$message, "\n")
    return(FALSE)
  })
}

# Try with archive package
zip_files <- list.files(pattern = '\\.zip$')

# S0100115 - Ф1 баланс
zip_files[1] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0100115.zip
extract_with_archive(zip_files[1], "S0100115_2024_as_for_2025-07-03")

# S0100215 - Ф2 звіт про фінансові результати в тисячах
zip_files[2] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0100215.zip
extract_with_archive(zip_files[2], "S0100215_2024_as_for_2025-07-03")

# S0100311 - Ф3 звіт рух грошових коштів
zip_files[3] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0100311.zip
extract_with_archive(zip_files[3], "S0100311_2024_as_for_2025-07-03")

# S0103355 - Ф3-н
zip_files[4] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0103355.zip
extract_with_archive(zip_files[4], "S0103355_2024_as_for_2025-07-03")

# S0104010 - Звіт про власний капітал
zip_files[5] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0104010.zip
extract_with_archive(zip_files[5], "S0104010_2024_as_for_2025-07-03")

# S0105009 - Ф5 - примітки до річної звітності
zip_files[6] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0105009.zip
extract_with_archive(zip_files[6], "S0105009_2024_as_for_2025-07-03")

# S0106007 Ф6 - Додаток до приміток до річної фінансової звітності
zip_files[7] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0106007.zip
extract_with_archive(zip_files[7], "S0106007_2024_as_for_2025-07-03")

# S0110014 - Фінансовий звіт малого підприємства за формами №1-м, 2-м
zip_files[8] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0110014.zip
extract_with_archive(zip_files[8], "S0110014_2024_as_for_2025-07-03")

# S0111007 - Фінансова звітність мікропідприємства за формами №1-мс, 2-мс
zip_files[9] # FG_2025-01-01_2025-07-03_Ð Ñ_Ðº_2024_S0111007.zip
extract_with_archive(zip_files[9], "S0111007_2024_as_for_2025-07-03")



    
# розпаковуємо xml, створюємо df

# Function to safely process a single XML file
safe_read_xml <- safely(function(file_name) {
  read_xml(file_name) |>
    as_list() |>
    as_tibble() |>
    unnest_longer(DECLAR) |>
    unnest_longer(DECLAR) |>
    pivot_wider(names_from = DECLAR_id, values_from = DECLAR)
})

# Function to process a batch of files
process_batch <- function(file_batch) {
  results <- map(file_batch, safe_read_xml)
  
  # Filter out failed attempts (where an error occurred)
  successful_results <- map(results, "result")
  successful_results <- compact(successful_results)  # Remove NULLs (failed files)
  
  bind_rows(successful_results)  # Combine successful tibbles
}

batch_size <- 1000  # Adjust based on your laptop's performance


# Ф2

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/S0100215_2024_as_for_2025-07-03")

f2_names <- list.files()

xmlParse(f2_names[1])

read_xml(f2_names[1]) |> 
  as_list() |>
  as_tibble() |>
  unnest_longer(DECLAR) |>
  unnest_longer(DECLAR) |> 
  pivot_wider(names_from = DECLAR_id, values_from = DECLAR)

# Split file names into smaller chunks

f2_names <- list.files()
file_batches <- split(f2_names, ceiling(seq_along(f2_names) / batch_size))

# Initialize progress bar
pb <- progress_bar$new(
  format = "  Processing [:bar] :percent in batch :batch of :total_batches | ETA: :eta",
  total = length(file_batches),    # Total number of batches
  width = 60
)

# Process each batch with a progress bar and skip corrupted files
f2_df <- map_dfr(seq_along(file_batches), function(i) {
  pb$tick(tokens = list(batch = i, total_batches = length(file_batches)))  # Update progress bar
  process_batch(file_batches[[i]])
})

# write.csv(f2_df,
#          file = "C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/csv_output/f2_2024_as_for_2025-07-03.csv",
#          row.names = FALSE)

# Ф2м, Ф1м

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/S0110014_2024_as_for_2025-07-03")

f2m_f1m_names <- list.files()
file_batches <- split(f2m_f1m_names, ceiling(seq_along(f2m_f1m_names) / batch_size))

# Initialize progress bar
pb <- progress_bar$new(
  format = "  Processing [:bar] :percent in batch :batch of :total_batches | ETA: :eta",
  total = length(file_batches),    # Total number of batches
  width = 60
)

# Process each batch with a progress bar and skip corrupted files
f2m_f1m_df <- map_dfr(seq_along(file_batches), function(i) {
  pb$tick(tokens = list(batch = i, total_batches = length(file_batches)))  # Update progress bar
  process_batch(file_batches[[i]])
})


# write_parquet(f2m_f1m_df,
#               "C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/csv_output/f2m_f1m_2024_as_for_2025-07-03.parquet")
# 


# Ф2мс, Ф1мс

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/S0111007_2024_as_for_2025-07-03")

f2ms_f1ms_names <- list.files()
file_batches <- split(f2ms_f1ms_names, ceiling(seq_along(f2ms_f1ms_names) / batch_size))

# Initialize progress bar
pb <- progress_bar$new(
  format = "  Processing [:bar] :percent in batch :batch of :total_batches | ETA: :eta",
  total = length(file_batches),    # Total number of batches
  width = 60
)

# Process each batch with a progress bar and skip corrupted files
f2ms_f1ms_df <- map_dfr(seq_along(file_batches), function(i) {
  pb$tick(tokens = list(batch = i, total_batches = length(file_batches)))  # Update progress bar
  process_batch(file_batches[[i]])
})

write_parquet(f2ms_f1ms_df,
 "C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/csv_output/f2ms_f1ms_2024_as_for_2025-07-03.parquet")
 


# Ф1

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/S0100115_2024_as_for_2025-07-03")

f1_names <- list.files()
file_batches <- split(f1_names, ceiling(seq_along(f1_names) / batch_size))

# Initialize progress bar
pb <- progress_bar$new(
  format = "  Processing [:bar] :percent in batch :batch of :total_batches | ETA: :eta",
  total = length(file_batches),    # Total number of batches
  width = 60
)

# Process each batch with a progress bar and skip corrupted files
f1_df <- map_dfr(seq_along(file_batches), function(i) {
  pb$tick(tokens = list(batch = i, total_batches = length(file_batches)))  # Update progress bar
  process_batch(file_batches[[i]])
})

# write.csv(f1_df,
#          file = "C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/фінзвітність 2024/csv_output/f1_2024_as_for_2025-07-03.csv",
#          row.names = FALSE)



# setwd("C:/Users/Mykola Kuzin/Desktop/BRDO/sme_internet_providers/data sources/fin_zvit_2023_ric_-2024-01-01_2024-09-08/f2/F-I_ric_2023_2024-01-01_2024-09-08")

# f1_names <- list.files()

# xmlParse(f1_names[1])

# read_xml(f1_names[1]) |> 
#  as_list() |>
#  as_tibble() |>
#  unnest_longer(DECLAR) |>
#  unnest_longer(DECLAR) |> 
#  pivot_wider(names_from = DECLAR_id, values_from = DECLAR)

# f2
