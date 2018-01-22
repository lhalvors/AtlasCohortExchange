# AtlasCohortExchange

R package to allow exchange of Atlas cohort inclusion information with external sites.

In the central Atlas instance, export cohort definition of interest as a JSON file using the AtlasCohortExchange-CohortDefExport Jupyter notebook.

Send the JSON file to the external client, who then creates a new cohort in their local ATLAS, using the provided JSON to re-create the same cohort defintion as in the central Atlas instance by using this R package. Once the inclusion report has been generated in the local Atlas instance, the exportCohortResults function in the R package can then be executed to export the inclusion report to a JSON file, which can be sent back to the central Atlas instance.

In the central Atlas instance, import the cohort inclusion report using the AtlasCohortExchange-ResultImport Jupyter notebook.
