% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/basic_tbl_tools.R
\name{tbl_correlations}
\alias{tbl_correlations}
\title{Correlation tibble}
\usage{
tbl_correlations(
  data,
  correlation_method = "pearson",
  correlation_used = "pairwise.complete.obs",
  diagonal = NA,
  remove_columns = NULL,
  include_logical = T,
  include_factors = F,
  character_to_factor = F,
  exclude_feature_columns = NULL,
  exclude_feature_correlated_columns = NULL,
  full_rank = T
)
}
\arguments{
\item{correlation_method}{a character string indicating which correlation coefficient (or covariance) is to be computed. One of "pearson" (default), "kendall", or "spearman": can be abbreviated.}

\item{correlation_used}{an optional character string giving a method for computing covariances in the presence of missing values. This must be (an abbreviation of) one of the strings "everything", "all.obs", "complete.obs", "na.or.complete", or "pairwise.complete.obs".}

\item{include_logical}{if `TRUE` includes logical features}

\item{include_factors}{if `TRUE` converts factors to variables}

\item{character_to_factor}{if `TRUE` converts character vectors to factor}

\item{full_rank}{if `TRUE` returns full rank dummy variables}
}
\value{

}
\description{
Correlation tibble
}
