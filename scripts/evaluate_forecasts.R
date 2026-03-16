#!/usr/bin/env Rscript

# evaluate_forecasts.R
# Computes evaluation metrics for disease forecasts based on configuration files.

# Ensure required packages are loaded
suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(yaml)
  library(ggplot2)
})

# 1. Setup Configuration
diseases <- c("covid", "flu", "rsv")
base_dir <- getwd()
model_dir <- file.path(base_dir, "model-output")
target_dir <- file.path(base_dir, "data", "targets")
output_dir <- file.path(base_dir, "_site", "resources", "evals")

# Ensure output directory exists
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# 2. Helper functions
load_config <- function(disease) {
  config_path <- file.path(base_dir, paste0("predevals-config-", disease, ".yml"))
  if (file.exists(config_path)) {
    return(yaml::read_yaml(config_path))
  }
  return(NULL)
}

# 3. Main Evaluation Loop
for (disease in diseases) {
  cat(sprintf("\n=== Processing %s ===\n", toupper(disease)))
  
  config <- load_config(disease)
  if (is.null(config)) {
    cat(sprintf("  No config found for %s. Skipping.\n", disease))
    next
  }
  
  disease_out_dir <- file.path(output_dir, disease)
  dir.create(disease_out_dir, recursive = TRUE, showWarnings = FALSE)
  
  # 3.1 Load Targets
  target_file <- file.path(target_dir, paste0(disease, "-targets.parquet"))
  if (!file.exists(target_file)) {
    cat(sprintf("  Target file not found for %s. Skipping.\n", disease))
    next
  }
  targets <- read_parquet(target_file)
  
  # 3.2 Load Forecasts
  # Find all model folders for this disease
  disease_model_dirs <- list.dirs(model_dir, recursive = FALSE)
  disease_model_dirs <- disease_model_dirs[grepl(paste0("^", disease, "-"), basename(disease_model_dirs))]
  
  all_forecasts <- list()
  for (mdir in disease_model_dirs) {
    csv_files <- list.files(mdir, pattern = "\\.csv$", full.names = TRUE)
    for (f in csv_files) {
      df <- read_csv(f, show_col_types = FALSE)
      # Extract model name from directory
      df$model <- basename(mdir)
      all_forecasts[[length(all_forecasts) + 1]] <- df
    }
  }
  
  if (length(all_forecasts) == 0) {
    cat(sprintf("  No forecasts found for %s. Skipping.\n", disease))
    next
  }
  
  forecasts <- bind_rows(all_forecasts)
  cat(sprintf("  Loaded %d target rows and %d forecast rows for %s.\n", 
              nrow(targets), nrow(forecasts), disease))
  
  # ---------------------------------------------------------------------
  # 3.3 INITIAL MOCK SCORING LOGIC (To be replaced with actual WIS metrics)
  # Keeping it lightweight for Step 1 to ensure pipeline architecture works
  # ---------------------------------------------------------------------
  
  # Mock calculation of WIS by horizon to confirm visualization routing
  wis_by_horizon <- forecasts %>%
    group_by(model, horizon) %>%
    summarize(mean_wis = runif(1, 10, 100), .groups = "drop") # Placeholder
    
  # 3.4 Save Data Tables
  write_csv(wis_by_horizon, file.path(disease_out_dir, "wis_by_horizon.csv"))
  
  # 3.5 Save Plots (WIS by Horizon)
  p <- ggplot(wis_by_horizon, aes(x = as.factor(horizon), y = mean_wis, fill = model)) +
    geom_bar(stat = "identity", position = "dodge") +
    theme_minimal() +
    labs(
      title = paste(toupper(disease), "Mean WIS by Horizon"),
      x = "Horizon",
      y = "Mean WIS (Placeholder)"
    ) +
    theme(legend.position = "bottom", legend.text = element_text(size = 6)) 
    
  ggsave(file.path(disease_out_dir, "wis_by_horizon.png"), plot = p, width = 8, height = 6)
  cat(sprintf("  Successfully saved outputs to %s.\n", disease_out_dir))
}

cat("\nEvaluation script completed.\n")
